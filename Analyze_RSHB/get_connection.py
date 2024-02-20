from sqlalchemy import create_engine
 
user = 'root'
password = 'password'
host = '127.0.0.1'
port = 3306
database = 'GeeksForGeeks'
dialect = 'oracle'
driver = 'jdbc'


def get_connection(driver:str, dialect:str, database:str, user:str, password:str, host:str, port:str):
    
    """get_connection

    Функция универсальная для запуска подключения к базе данных

    Returns: Статус о подключении
        
    """
    
    try:
        connect =  create_engine(
            url="{0}+{1}://{2}:{3}@{4}:{5}/{6}".format(
                dialect, driver, user, password, host, port, database
            )
         ) 
        return 'Подлкючение прошло успешно'
    except:
        return 'Ошибка'
    

    
