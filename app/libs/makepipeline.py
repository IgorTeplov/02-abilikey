import asyncio
from json import dumps, loads
from datetime import datetime
from libs.logs import logger, execution_logger, error_logger, EXEC_ID
from traceback import format_exc
from libs.stack import PrintedStack
from libs.dirs import PIPE_DIR
import atexit
import signal

INDENT = 4
OUT = PrintedStack(15, True)


class Step:
    def __init__(self, handler: 'coro', const: 'Const|None' = None, name: 'str' = '', isloop: 'bool|str' = False, mapper: "str|None" = None):
        self.handler = handler
        self.const = const
        self.name = name
        self.isloop = isloop
        if self.isloop not in [False, 'loop', 'aloop']:
            error = f'Bad Loop Flag: {self.isloop} [False, "loop", "aloop"]'
            error_logger.error(error)
            raise Exception(error)

        self.mapper = mapper
        if not isinstance(self.mapper, str) and self.mapper is not None:
            error = f'Mapper must be string!'
            error_logger.error(error)
            raise Exception(error)

    def get_map(self, out):
        if self.mapper is None:
            return out
        else:
            parts = self.mapper.split('.')
            for part in parts:
                try:
                    out = out.get(part)
                except Exception as e:
                    error = f'Can\'t map {e}, {out.keys()}, {self.mapper}'
                    error_logger.error(error)
                    raise Exception(error)
            return out

    async def _run(self, pipe: 'Pipeline', data: 'dict|list|None' = None, context: 'Const|None' = None, iteration=None, step_number=None) -> 'dict|list|None':
        out = await self.handler(data=data, context=context, const=self.const, pipe=pipe, iteration=iteration, step_number=step_number)
        if isinstance(out, (dict, list, tuple)) or out is None:
            return out
        else:
            error = f"Bad Step Execution: out={out}"
            error_logger.error(error)
            raise Exception(error)

    async def run(self, pipe: 'Pipeline', data: 'dict|list|None' = None, context: 'Const|None' = None, iteration=None, step_number=None, root_pipe: 'Pipeline' = None) -> 'dict|list|None':
        if iteration is not None:
            root_pipe.current_loop['active'] += 1
            try:
                OUT.print(f'Start A Iteration {iteration}')
                answer = await self._run(pipe, data, context, iteration=iteration, step_number=step_number)
                OUT.print(f'End A Iteration {iteration}')
            except Exception:
                error = f'Error in iteration {iteration}'+'\n'+f'{format_exc()}'
                OUT.print(error)
                error_logger.error(error)
                answer = None
            root_pipe.current_loop['active'] -= 1
            root_pipe.current_loop['finished'] += 1
        else:
            try:
                answer = await self._run(pipe, data, context)
            except Exception as e:
                error = f'Error in step {self.name}'+'\n'+f'{format_exc()}'
                OUT.print(error)
                error_logger.error(error)
                raise e
        return answer

    def __gt__(self, other):
        pipe = Pipeline()
        pipe.steps.append(self)
        pipe.steps.append(other)
        return pipe

    def __str__(self):
        return self.name

    def __repr__(self):
        return self.__str__()


class Context:
    def __init__(self, id_):
        self.id_ = id_

    def __getitem__(self, key):
        data_src = PIPE_DIR / f'{str(self.id_)}/step_{key}.json'
        if data_src.is_file():
            return loads(data_src.read_text())


class Pipeline:
    def __init__(self, context: 'Context|None' = None):
        self.id = EXEC_ID
        self.name = None
        self._dir = PIPE_DIR / str(self.id)
        self._dir.mkdir()
        self.steps = []
        self.context = context
        self.state = {}
        if context is None:
            self.context = Context(self.id)
        self.exec = None
        self.exec_steps = []

        self.time_handler = None
        self.time_run = True
        self.current_loop = None

        self.global_start = None
        self.elapsed = None
        self.last_status = None
        signal.signal(signal.SIGINT, self.exit)
        atexit.register(self.atexit)

    def exit(self, signum, frame):
        self.last_status = 'killed'
        exit(1)

    def atexit(self):
        if self.last_status is None:
            self.last_status = 'finished'
        if self.time_run:
            self.time_run = False
        self.write_status(self.get_status(self.last_status))

    def write_status(self, status):
        (self._dir / "status.json").write_text(dumps(status, indent=4))

    def get_status(self, step=None):
        return {
            'id': str(self.id),
            'name': self.name,
            'start': f'{self.global_start}',
            'duration': f'{datetime.now() - self.global_start}',
            'finished': not self.time_run,
            'last_step': step
        }

    def set_time_handler(self, time_handler):
        self.time_handler = time_handler

    def set_exec(self, exec_, steps):
        self.exec = exec_
        self.exec_steps = steps

    async def timer(self):
        start = datetime.now()
        while self.time_run:
            if self.time_handler:
                elapsed = datetime.now() - start
                try:
                    await self.time_handler(self, elapsed)
                except Exception:
                    error_logger.error(f'{format_exc()}')
                    break
            await asyncio.sleep(1)

    async def run(self, name="Undefined Process"):
        try:
            step_out = None
            self.name = name
            OUT.print(f'Start Execution: {name} with id: {self.id}')
            logger.info(f'Start Execution: {name} with id: {self.id}')
            self.global_start = datetime.now()
            self.write_status(self.get_status())
            for i, step in enumerate(self.steps):
                self.write_status(self.get_status(f'{step.name} [{i+1}]'))
                OUT.print(f'Start Step {i} {step.name}')
                logger.info(f'Start Step {i} {step.name}')
                local_start = datetime.now()
                context = {'in': step_out, 'out': None}
                if self.exec is None or f"step_{i+1}" not in self.exec_steps:
                    if step.isloop is False:
                        step_out = await step.run(data=step_out, context=self.context, pipe=self.state, step_number=i)
                    elif step.isloop == 'loop' and isinstance(step.get_map(step_out), (list, tuple)):
                        _step_out = []
                        self.current_loop = {
                            'name': step.name,
                            'max': len(step.get_map(step_out)),
                            'active': 0,
                            'finished': 0
                        }
                        for j, item in enumerate(step.get_map(step_out)):
                            data = await step.run(data=item, context=self.context, pipe=self.state, iteration=j, step_number=i, root_pipe=self)
                            if data is not None:
                                _step_out.append(data)
                            execution_logger.info(f'End {j} iteration')
                        step_out = _step_out
                        self.current_loop = None
                    elif step.isloop == 'aloop' and isinstance(step.get_map(step_out), (list, tuple)):
                        tasks = []
                        self.current_loop = {
                            'name': step.name,
                            'max': len(step.get_map(step_out)),
                            'active': 0,
                            'finished': 0
                        }
                        for j, item in enumerate(step.get_map(step_out)):
                            tasks.append(step.run(data=item, context=self.context, pipe=self.state, iteration=j, step_number=i, root_pipe=self))
                        step_out = await asyncio.gather(*tasks)
                        step_out = [item for item in step_out if item is not None]
                        self.current_loop = None
                    elif step.isloop in ['loop', 'aloop'] and not isinstance(step.get_map(step_out), (list, tuple)):
                        error = f'Bad input data: {step_out}'
                        error_logger.error(error)
                        raise Exception(error)
                else:
                    OUT.print(f"Load out from exec {self.exec}")
                    step_out = loads((PIPE_DIR / str(self.exec) / f'step_{i+1}.json').read_text())["out"]

                context['out'] = step_out
                (self._dir / f'step_{i+1}.json').write_text(
                    dumps(context, indent=INDENT)
                )
                OUT.print(f'Step {i} {step.name} Duration: {datetime.now() - local_start}')
                logger.info(f'Step {i} {step.name} Duration: {datetime.now() - local_start}')
                self.write_status(self.get_status(f'{step.name}[{i}]'))

            OUT.print(f'Duration: {datetime.now() - self.global_start}')
            OUT.print(f'End Execution {self.id}')
            logger.info(f'Duration: {datetime.now() - self.global_start}')
            logger.info(f'End Execution {self.id}')
            self.time_run = False
            return step_out, self.context
        except Exception:
            error_logger.error(f'{format_exc()}')
            self.last_status = 'fatal error'
            exit(1)

    async def run_with_timer(self, name="Undefined Process"):
        await asyncio.gather(
            self.run(name),
            self.timer()
        )

    def __gt__(self, other):
        self.steps.append(other)
        return self

    def __le__(self, other):
        self.steps.append(other)
        return self

    def __str__(self):
        return f'Pipe({self.id})'

    def __repr__(self):
        return self.__str__()


class Const:
    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)

    def __repr__(self):
        return 'const'



if __name__ == '__main__':
    async def _step1(data, context, const, pipe, iteration, step_number):
        # Get Users
        return [
            {'id': 1, 'user': 'A', 'likes': 24},
            {'id': 2, 'user': 'B', 'likes': 55},
            {'id': 3, 'user': 'C', 'likes': 132}
        ]

    async def _step2(data, context, const, pipe, iteration, step_number):
        # filter user with likes > 50
        out = []
        for item in data:
            if item['likes'] > 50:
                out.append(item)
        return out

    async def _alt_step2(data, context, const, pipe, iteration, step_number):
        # filter user with likes > 50
        # alt version for loops and aloops
        execution_logger.info(f'{data}, {data["likes"] > 50}')
        if data['likes'] > 50:
            return data

    async def _step3(data, context, const, pipe, iteration, step_number):
        # print items
        for item in data:
            OUT.print(item)

    step1 = Step(_step1, name='step1')
    step2 = Step(_alt_step2, name='step2', const=Const(key=0.7), isloop='loop')
    step3 = Step(_step3, name='step3')

    pipe = step1 > step2
    pipe <= step3

    asyncio.run(pipe.run_with_timer())
