from flask import request
from flask_restful import Resource

from mysql_connection import get_connection
from mysql.connector import Error
from flask_jwt_extended import jwt_required
from flask_jwt_extended import get_jwt_identity

class MovieListResource(Resource) :
    # 비회원과 회원을 구분 ->> optional= 
    # 헤더가 있으면 유저아이디를 가져오고
    # 헤더가 없으면 None로 가져온다.
    @jwt_required(optional=True)
    def get(self) :
        user_id = get_jwt_identity()

        print("유저 아이디")
        print(user_id)


        order=request.args.get('order')
        offset=request.args.get('offset')
        limit=request.args.get('limit')

        try :
            connection = get_connection()

            if user_id is None :

                query = '''select m.id,m.title,ifnull(count(r.movie_id),0) as cnt,ifnull(avg(r.rating) , 0) as avg
                        from movie m
                        left join rating r
                        on r.movie_id = m.id
                        group by m.id
                        order by '''+order+''' desc
                        limit '''+offset+''','''+limit+''';'''

                cursor = connection.cursor(dictionary=True)

                cursor.execute(query)
            else :
                query = '''select m.id,m.title,ifnull(count(r.movie_id),0) as cnt,
                        ifnull(avg(r.rating) , 0) as avg,if(f.user_id is null,0,1) as is_favorite
                        from movie m
                        left join rating r
                        on r.movie_id = m.id
                        left join favorite f
                        on m.id = f.movie_id and f.user_id = %s
                        group by m.id
                        order by '''+order+''' desc
                        limit '''+offset+''','''+limit+''';'''
                
                record = (user_id,)

                cursor = connection.cursor(dictionary=True)

                cursor.execute(query,record)


            result_list=cursor.fetchall()

            i = 0
            for row in result_list :
                result_list[i]['avg'] = float(row['avg'])
                i = i+1

            print(result_list)
            

            cursor.close()
            connection.close()
        except Error as e :
            print(e)
            cursor.close()
            connection.close()
            return{"result":"fail","error":str(e)}, 500
        


        return {"result" : 'seccess','items':result_list,'count':len(result_list)}, 200


class MovieSearchResource(Resource) :
    def get(self) :
        keyword=request.args.get('keyword')
        offset=request.args.get('offset')
        limit=request.args.get('limit')

        try :
            connection = get_connection()

            query = '''select m.id,m.title,ifnull(count(r.movie_id),0) as cnt,ifnull(avg(r.rating) , 0) as avg
                    from movie m
                    left join rating r
                    on r.movie_id = m.id
                    where title like '%'''+ keyword +'''%'
                    group by m.id
                    order by m.title
                    limit '''+ offset +''','''+ limit +''';'''

            ## 중요!!!! select 문은 
            ## 커서를 가져올 때 dictionary = True로 해준다
            cursor = connection.cursor(dictionary=True)

            cursor.execute(query)

            result_list=cursor.fetchall()

            i = 0
            for row in result_list :
                result_list[i]['avg'] = float(row['avg'])
                i = i+1

            print(result_list)
            

            cursor.close()
            connection.close()
        except Error as e :
            print(e)
            cursor.close()
            connection.close()
            return{"result":"fail","error":str(e)}, 500
        


        return {"result" : 'seccess','items':result_list,'count':len(result_list)}, 200

class MovieResource(Resource) :
    @jwt_required(optional=True)
    def get(self,movie_id) :

        try :
            connection = get_connection()
            query = '''select m.* , ifnull(count(r.movie_id),0) as cnt,ifnull(avg(r.rating),0) as avg
                    from movie m
                    left join rating r
                    on m.id = r.movie_id
                    where m.id = %s;'''
            record = (movie_id,)
            cursor = connection.cursor(dictionary=True)

            cursor.execute(query,record)

            result_list=cursor.fetchall()

            if result_list[0]['id'] is None :
                return {'error':'잘못된 영화 아이디입니다.'},400

            i = 0
            for row in result_list :
                result_list[i]['avg'] = float(row['avg'])
                result_list[i]['year'] = row['year'].isoformat()
                i = i+1

            print(result_list)
            

            cursor.close()
            connection.close()
        except Error as e :
            print(e)
            cursor.close()
            connection.close()
            return{"result":"fail","error":str(e)}, 500
        


        return {"result" : 'success','movie':result_list[0]}, 200


