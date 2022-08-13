import os
from distutils.util import strtobool

from dotenv import load_dotenv

load_dotenv()

DATAGREPPER_URL = os.getenv("DATAGREPPER_URL")
PROD_ENVIRONMENT = bool(strtobool(os.getenv("PROD_ENVIRONMENT", 0)))

print(type(PROD_ENVIRONMENT))
print(PROD_ENVIRONMENT)
