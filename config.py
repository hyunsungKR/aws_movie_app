class Config :
    HOST = 'yh-db.cy1i4s2uewlm.ap-northeast-2.rds.amazonaws.com'
    DATABASE = 'movie_db2'
    DB_USER = 'movie_db_user'
    DB_PASSWORD = 'yh1234db'
    SALT = 'dskj29jcdn12jn'

    # JWT 관련 변수 셋팅
    JWT_SECRET_KEY = 'yhacdemy20230105##hello'
    JWT_ACCESS_TOKEN_EXPIRES = False
    PROPAGATE_EXCEPTIONS = True