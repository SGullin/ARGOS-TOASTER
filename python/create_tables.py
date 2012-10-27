#!/usr/bin/env python
import database
import utils

def main():
    db = database.Database()
    db.metadata.create_all(db.engine)

if __name__=='__main__':
    parser = utils.DefaultArguments(\
                description="Create TOASTER database tables.")
    args = parser.parse_args()
    main()
