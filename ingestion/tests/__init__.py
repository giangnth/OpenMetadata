from starrocks.sqlalchemy.datatype import ARRAY
from sqlalchemy.sql.sqltypes import VARCHAR, ARRAY as ARR

if __name__ == "__main__":
    arr = ARRAY()
    arr2 = ARR(VARCHAR)

    print(arr.__visit_name__, arr.python_type())
    print(arr2.__visit_name__, arr2.python_type)
