#!/usr/bin/env python
import database
import utils

def main():
    engine = database.get_toaster_engine()
    database.schema.metadata.create_all(engine)


if __name__=='__main__':
    parser = utils.DefaultArguments(\
                description="Create TOASTER database tables.")
    args = parser.parse_args()
    main()
