"""Added bookkeaping field

Revision ID: 48044ce97c4f
Revises: 3c99135f0c68
Create Date: 2013-03-15 17:08:13.950938

"""

# revision identifiers, used by Alembic.
revision = '48044ce97c4f'
down_revision = '3c99135f0c68'

from alembic import op
import sqlalchemy as sa


def upgrade():
    ### commands auto generated by Alembic - please adjust! ###
    op.add_column('bugmonths', sa.Column('_age_in_months', sa.Float(), nullable=True))
    ### end Alembic commands ###


def downgrade():
    ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('bugmonths', '_age_in_months')
    ### end Alembic commands ###
