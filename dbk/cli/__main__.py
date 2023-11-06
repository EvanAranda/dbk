import os
import traceback

from dbk.cli import main
from dbk.errors import DbkError

env = os.getenv("DBK_ENV", "dev")

try:
    main()
except DbkError as e:
    print(str(e))
    if env == "dev":
        traceback.print_exc()
except Exception as e:
    print("An unexpected error occurred.")
    if env == "dev":
        traceback.print_exc()
