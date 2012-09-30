import sqlalchemy as sa

import config

# Create the database engine
engine = sa.create_engine(config.dburl, echo=True)

# The database description (metadata)
metadata = sa.MetaData()

# Create users table
sa.Table('users', metadata, \
        sa.Column('user_id', sa.Integer, primary_key=True, \
                    autoincrement=True), \
        sa.Column('user_name', sa.String(64), nullable=False), \
        sa.Column('real_name', sa.String(64), nullable=False), \
        sa.Column('email_address', sa.String(64), nullable=False), \
        sa.Column('passwd_hash', sa.String(64)), \
        mysql_engine='InnoDB')
