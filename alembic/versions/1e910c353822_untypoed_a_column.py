"""untypoed a column

Revision ID: 1e910c353822
Revises: 52d489d2daa9
Create Date: 2013-04-30 11:34:26.855849

"""

# revision identifiers, used by Alembic.
revision = '1e910c353822'
down_revision = '52d489d2daa9'

from alembic import op
import sqlalchemy as sa


def upgrade():
    ### commands auto generated by Alembic - please adjust! ###
    op.add_column('bugmonths', sa.Column('assignee_nirc_undirected_cumulative', sa.Integer(), nullable=True))
    ### end Alembic commands ###


def downgrade():
    ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('bugmonths', 'assignee_nirc_undirected_cumulative')
    ### end Alembic commands ###
