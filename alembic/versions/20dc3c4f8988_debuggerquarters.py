"""debuggerquarters

Revision ID: 20dc3c4f8988
Revises: 25346a41c2df
Create Date: 2013-06-30 14:34:11.154073

"""

# revision identifiers, used by Alembic.
revision = '20dc3c4f8988'
down_revision = '25346a41c2df'

from alembic import op
import sqlalchemy as sa


def upgrade():
    ### commands auto generated by Alembic - please adjust! ###
    op.create_table('debuggerquarters',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('dbid', sa.Integer(), nullable=True),
    sa.Column('first', sa.Date(), nullable=True),
    sa.Column('constraint', sa.Float(), nullable=True),
    sa.Column('closeness', sa.Float(), nullable=True),
    sa.Column('clustering', sa.Float(), nullable=True),
    sa.Column('indegree', sa.Integer(), nullable=True),
    sa.Column('outdegree', sa.Integer(), nullable=True),
    sa.Column('betweenness', sa.Float(), nullable=True),
    sa.Column('effective_size', sa.Float(), nullable=True),
    sa.Column('efficiency', sa.Float(), nullable=True),
    sa.Column('churn', sa.Integer(), nullable=True),
    sa.Column('alter_churn', sa.Integer(), nullable=True),
    sa.Column('nreported', sa.Integer(), nullable=True),
    sa.Column('effective_size_churn', sa.Integer(), nullable=True),
    sa.ForeignKeyConstraint(['dbid'], ['debuggers.id'], ),
    sa.PrimaryKeyConstraint('id')
    )
    ### end Alembic commands ###


def downgrade():
    ### commands auto generated by Alembic - please adjust! ###
    op.drop_table('debuggerquarters')
    ### end Alembic commands ###
