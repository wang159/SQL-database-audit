from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.mysql import *
from sqlalchemy import Column

Base = declarative_base()

# SQL Alchemy uses "SHOW" command for autoload queries. If the user does not have
# access to the "SHOW" command, a custom table must be built as shown below

'''
sqlalchemy column data types. From dir(sqlalchemy.dialects.mysql.types)

'BIGINT', 'BIT', 'CHAR', 'DATETIME', 'DECIMAL', 'DOUBLE', 'FLOAT', 'INTEGER', 
'LONGBLOB', 'LONGTEXT', 'MEDIUMBLOB', 'MEDIUMINT', 'MEDIUMTEXT', 'NCHAR', 'NUMERIC', 
'NVARCHAR', 'REAL', 'SMALLINT', 'TEXT', 'TIME', 'TIMESTAMP', 'TINYBLOB', 'TINYINT', 
'TINYTEXT', 'VARCHAR', 'YEAR'
'''



'''
+---------------+--------------+------+-----+---------+----------------+
| Field         | Type         | Null | Key | Default | Extra          |
+---------------+--------------+------+-----+---------+----------------+
| id            | int(11)      | NO   | PRI | NULL    | auto_increment |
| name          | varchar(255) | NO   | MUL |         |                |
| username      | varchar(150) | NO   | UNI | NULL    |                |
| email         | varchar(100) | NO   | MUL |         |                |
| usertype      | varchar(25)  | NO   | MUL |         |                |
| block         | tinyint(4)   | NO   | MUL | 0       |                |
| approved      | tinyint(4)   | NO   |     | 2       |                |
| sendEmail     | tinyint(4)   | YES  |     | 0       |                |
| registerDate  | datetime     | YES  |     | NULL    |                |
| lastvisitDate | datetime     | YES  |     | NULL    |                |
| activation    | int(11)      | NO   |     | 0       |                |
| params        | text         | NO   |     | NULL    |                |
| lastResetTime | datetime     | YES  |     | NULL    |                |
| resetCount    | int(11)      | NO   |     | 0       |                |
+---------------+--------------+------+-----+---------+----------------+
''' 
class table_jos_users(Base):
  __tablename__ = 'jos_users'

  id            = Column(INTEGER, primary_key = True)
  name          = Column(VARCHAR)
  username      = Column(VARCHAR, unique = True)
  email         = Column(VARCHAR)
  usertype      = Column(VARCHAR)
  block         = Column(INTEGER)
  approved      = Column(INTEGER)
  sendEmail     = Column(INTEGER)
  registerDate  = Column(DATETIME)
  lastvisitDate = Column(DATETIME)
  activation    = Column(INTEGER)
  params        = Column(TEXT)
  lastResetTime = Column(DATETIME)
  resetCount    = Column(INTEGER)


