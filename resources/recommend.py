from flask import request
from flask_restful import Resource

from mysql_connection import get_connection
from mysql.connector import Error
from flask_jwt_extended import jwt_required
from flask_jwt_extended import get_jwt_identity
import pandas as pd

class MovieRecommendResource(Resource) :
    @jwt_required()
    
    def get(self) :
        # 1. 클라이언트로부터 데이터를 받아온다.
        user_id=get_jwt_identity()
        # 쿼리 스트링으로 받는 데이터는
        # 전부 문자열로 처리된다!!!!!
        number=int(request.args.get('number'))

        # 2. 추천을 위한 상관계수 데이터프레임을 읽어온다.
        movie_correlations=pd.read_csv('data/movie_correlations.csv',index_col='title')

        # 3. 이 유저의 별점 정보를 가져온다. = > 디비에서 가져온다.
        try :
            connection = get_connection()

            query = '''select m.title,r.rating
                    from rating r
                    left join movie m
                    on m.id = r.movie_id
                    where r.user_id = %s;'''
            record = (user_id,)

            cursor = connection.cursor(dictionary=True)
            cursor.execute(query,record)
            result_list = cursor.fetchall()

            print(result_list)

            cursor.close()
            connection.close()

        except Error as e :
            print(e)
            cursor.close()
            connection.close()
            return{"result":"fail","error":str(e)}, 500
        
        # 4. DB로부터 가져온 내 별점 정보를 
        #    데이터 프레임으로 만들어준다.
        my_rating=pd.DataFrame(data=result_list)
        
        
        # 5. 내 별점 정보 기반으로 추천영화 목록을 만든다.
        similar_movies_list = pd.DataFrame()
        for i in range(my_rating.shape[0]):
            movie_title=my_rating['title'][i]
            similar_movie=movie_correlations[movie_title].dropna().sort_values(ascending=False).to_frame()
            similar_movie.columns = ['Correlation']
            similar_movie['weight']=my_rating['rating'][i] * similar_movie['Correlation']
            similar_movies_list=similar_movies_list.append(similar_movie)

        # 6. 내가 본 영화 제거
        drop_index_list=my_rating['title'].to_list()
        for name in drop_index_list :
            if name in similar_movies_list.index :
                similar_movies_list.drop(name,axis=0,inplace=True)
        
        # 7. 중복 추천된 영화는 weight값이 가장 큰 값으로만
        #    중복 제거한다.

        recomm_movie_list=similar_movies_list.groupby('title')['weight'].max().sort_values(ascending=False).head(number)

        


    

        # 8. JSON으로 클라이언트에 보내야한다.
        recomm_movie_list=recomm_movie_list.to_frame()
        recomm_movie_list=recomm_movie_list.reset_index()
        recomm_movie_list=recomm_movie_list.to_dict('records')

        
        
        return {'result':'success','itmes':recomm_movie_list,'count':len(recomm_movie_list)},200


class MovieRecommendRealTimeResource(Resource) :

    @jwt_required()
    def get(self) :

        user_id=get_jwt_identity()
        number=int(request.args.get('number'))

        try :
            connection = get_connection()
            query = '''select m.title, r.user_id, r.rating
                    from movie m
                    left join rating r
                    on m.id = r.movie_id;'''
            cursor = connection.cursor(dictionary=True)
            cursor.execute(query)
            result_list = cursor.fetchall()

            df = pd.DataFrame(data=result_list)
            df=df.pivot_table(index='user_id',columns='title',values='rating')


            movie_correlations = df.corr(min_periods=50)

            # 내 별점 정보를 가져와야 나의 맞춤형 추천 가능
            query = '''select m.title,r.rating
                    from rating r
                    left join movie m
                    on m.id = r.movie_id
                    where r.user_id = %s;'''
            record = (user_id,)

            cursor = connection.cursor(dictionary=True)
            cursor.execute(query,record)
            result_list = cursor.fetchall()

            print(result_list)

            cursor.close()
            connection.close()

        except Error as e :
            print(e)
            cursor.close()
            connection.close()
            return{"result":"fail","error":str(e)}, 500
        
        # 4. DB로부터 가져온 내 별점 정보를 
        #    데이터 프레임으로 만들어준다.
        my_rating=pd.DataFrame(data=result_list)
        
        
        # 5. 내 별점 정보 기반으로 추천영화 목록을 만든다.
        similar_movies_list = pd.DataFrame()
        for i in range(my_rating.shape[0]):
            movie_title=my_rating['title'][i]
            similar_movie=movie_correlations[movie_title].dropna().sort_values(ascending=False).to_frame()
            similar_movie.columns = ['Correlation']
            similar_movie['weight']=my_rating['rating'][i] * similar_movie['Correlation']
            similar_movies_list=similar_movies_list.append(similar_movie)

        # 6. 내가 본 영화 제거
        drop_index_list=my_rating['title'].to_list()
        for name in drop_index_list :
            if name in similar_movies_list.index :
                similar_movies_list.drop(name,axis=0,inplace=True)
        
        # 7. 중복 추천된 영화는 weight값이 가장 큰 값으로만
        #    중복 제거한다.

        recomm_movie_list=similar_movies_list.groupby('title')['weight'].max().sort_values(ascending=False).head(number)

        


    

        # 8. JSON으로 클라이언트에 보내야한다.
        recomm_movie_list=recomm_movie_list.to_frame()
        recomm_movie_list=recomm_movie_list.reset_index()
        recomm_movie_list=recomm_movie_list.to_dict('records')

        
        
        return {'result':'success','itmes':recomm_movie_list,'count':len(recomm_movie_list)},200
