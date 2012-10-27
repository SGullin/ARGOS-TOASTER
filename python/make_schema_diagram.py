#!/usr/bin/env python

"""A script for creating schema diagrams of the toaster database.
    
    Requires: 
        - pygraphviz python module (http://networkx.lanl.gov/pygraphviz/)
        - Graphviz (http://www.graphviz.org/)
        - sadisplay python module (http://pypi.python.org/pypi/sadisplay)
"""
import sys

import pygraphviz as pgv
import sadisplay.describe

import database
import config



def main():
    db = database.Database()
    tables = db.tables
    table_names = set(tables.keys())

    # Not quite sure what this line does (got it from sadisplay.reflect
    desc = sadisplay.describe(db.tables.values())

    graph = pgv.AGraph(string=sadisplay.render.dot(desc))
    graph.layout(prog='dot', args="-Gsplines=0 -Earrowhead=vee -Glabel='TOASTER!' -Gfontsize=14")
    graph.draw('toast_schema.pdf')


if __name__=='__main__':
    main()
