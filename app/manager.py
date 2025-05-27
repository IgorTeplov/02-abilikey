from argparse import ArgumentParser
from libs.manager import manager


def main():
    parser = ArgumentParser()
    parser.add_argument(
        '-c', choices=['execs', 'exec', 'clear', 'active', 'today'], required=True
    )
    parser.add_argument(
        '-a'
    )
    argv = parser.parse_args()

    if argv.c == 'execs':
        manager.get_execs(True)
    elif argv.c == 'exec' and argv.a is not None:
        manager.get_exec(argv.a)
    elif argv.c == 'clear' and argv.a is not None and argv.a.isdigit():
        manager.clear_old_record(int(argv.a))
    elif argv.c == 'active':
        manager.show_active()
    elif argv.c == 'today':
        manager.show_today()


if __name__ == '__main__':
    main()
