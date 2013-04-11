"""more dm vars

Revision ID: 5463e2520d03
Revises: 40c950a09f42
Create Date: 2013-04-10 11:16:10.704232

"""

# revision identifiers, used by Alembic.
revision = '5463e2520d03'
down_revision = '40c950a09f42'

from alembic import op
import sqlalchemy as sa


def upgrade():
    ### commands auto generated by Alembic - please adjust! ###
    op.add_column('debuggermonths', sa.Column('efficiency', sa.Float(), nullable=True))
    op.add_column('debuggermonths', sa.Column('nreported', sa.Integer(), nullable=True))
    op.add_column('debuggermonths', sa.Column('churn', sa.Integer(), nullable=True))
    ### end Alembic commands ###


def downgrade():
    ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('debuggermonths', 'churn')
    op.drop_column('debuggermonths', 'nreported')
    op.drop_column('debuggermonths', 'efficiency')
    ### end Alembic commands ###
