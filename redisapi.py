from redisearch import Client, TextField, NumericField, Query
import sys
import MySQLdb
import redis


class MysqlRedis():
    # localhost='127.0.0.1'
    redis_host = 'localhost'
    redis_port = 6379
    redis_password = ''
    redis_encoding = 'utf8'

    mysql_host = '127.0.0.1'
    mysql_user = 'root'
    mysql_password = ''
    mysql_port = 3306
    mysql_db = 'Checkout'
    db_connection = None
    redis_connection = None

    def connect(self):
        try:
            self.db_connection = MySQLdb.connect(host=self.mysql_host, user=self.mysql_user,
                                                 password=self.mysql_password,
                                                 port=self.mysql_port, db=self.mysql_db)

            print('Succesfully Connected to MySql Server')
        except Exception as e:
            print(e)

        try:
            self.redis_connection = redis.StrictRedis(host=self.redis_host, port=self.redis_port,
                                                      password=self.redis_password,
                                                      encoding=self.redis_encoding,
                                                      decode_responses=True)

            self.redis_connection.set("message", "Successfully Connected to Redis")
            msg = self.redis_connection.get("message")
            print(msg)
        except Exception as e:
            print('errror::: %s' % e)
        return self.db_connection, self.redis_connection

    def disconnect(self):
        self.db_connection.close()
        self.db_connection = None

    def mysql_redis(self):
        db_connection, _ = self.connect()
        cursor = db_connection.cursor()
        cursor.execute('SELECT * FROM customers')
        results = cursor.fetchall()

        for x in results:
            print(x)
            break
        r_redis = self.redis_connection
        r_redis.delete('all_records')
        for row in results:
            r_redis.rpush('all_records', row[3])

        cursor.close()

        self.disconnect()

    def get_data_from_redis(self):
        _, redis_connection = self.connect()
        r_redis = self.redis_connection
        data = []
        data = r_redis.lrange('all_records', 0, 100)
        print(data)

    # sql query "~"+you+"~"
    def clientpush(self):
        client = Client('Checkout')

        client.create_index(
            [NumericField('Key'), TextField('UsageClass'), TextField('CheckoutType'), TextField('MaterialType'),
             NumericField('CheckoutYear'), NumericField('CheckoutMonth'), NumericField('Checkouts'), TextField('Title'),
             TextField('Creator'), TextField('Subjects'), TextField('Publisher'), TextField('PublicationYear')])

        db_connection, _ = self.connect()
        cursor = db_connection.cursor()
        cursor.execute('SELECT * FROM customers')
        results = cursor.fetchall()
        i=0
        for result in results:
            client.add_document('doc%s'%i, Key=result[0], UsageClass=result[1], CheckoutType=result[2], MaterialType=result[3],
                                CheckoutYear=result[4], CheckoutMonth=result[5], Checkouts=result[6],
                                Title=result[7], Creator=result[8], Subjects=result[9], Publisher=result[10], PublicationYear=result[11])
            i+=1
            print(i)
        res = client.search('BOOK')

        print("{}   {}".format(res.total, res.docs[0].Title))
        res1 = client.search("use")
        print(res1)
        q = Query('use').verbatim().no_content().paging(0, 5)
        res1 = client.search(q)
        print(res1)
        cursor.close()
        db_connection.close()


print("\n\n\n\n")
convert = MysqlRedis()
convert.mysql_redis()
convert.get_data_from_redis()
convert.clientpush()