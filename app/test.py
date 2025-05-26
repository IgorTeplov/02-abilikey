from load_dotenv import load_dotenv
import os
load_dotenv()

print(os.environ.get('HAHA', None))
