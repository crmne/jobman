from sqlalchemy.sql.expression import column
from sqlalchemy.orm import aliased

def crazy_sql_command(viewname, cols, dicttab, keytab, id_col='id', dict_id='dict_id'):

    #create or replace view expview as select * from (select id as v1_id, value as nhid from
    #test0 where name='nhid') nhid LEFT OUTER JOIN (select id as v2_id, value as lrate from
    #test0 where name='lrate') lrate on nhid.v1_id = lrate.v2_id;

    col_queries = []
    colname0 = None
    for i, (colname, table_col) in enumerate(cols):
        safe_col = colname.replace('_','')   # get rid of underscores
        safe_col = safe_col.replace('.','_') # replace dots with underscores
      
        cols[i][0] = safe_col

        q = """LEFT OUTER JOIN 
               (select %(dict_id)s, %(table_col)s as %(safe_col)s from \"%(keytab)s\"
                where name='%(colname)s') %(safe_col)s 
                on %(safe_col)s.dict_id = %(dicttab)s.%(id_col)s""" % locals()
        
        col_queries.append(q)
       
    header = "create or replace view %s as select %s.%s, %s from %s " \
            % (viewname, dicttab, id_col, (", ".join([c[0] for c in cols])), dicttab)

    rval = header + "\n".join(col_queries)

    return rval

def create_view(db_handle, viewname):

    s = db_handle.session()

    # Get column names
    kv = db_handle._KeyVal
    d = db_handle._Dict
    name_query = s.query(kv.name, kv.type).distinct()

    safe_names = []
    sub_queries = []
    for name, type in name_query.all():
        safe_name = name.replace('_','').replace('.','_')
        sub_query = s.query(kv.dict_id, column(type+'val').label(safe_name))\
                .filter_by(name = name)\
                .subquery()

        safe_names.append(safe_name)
        sub_queries.append(sub_query)

    # Crazy query
    main_query = s.query(d.id, *[column(name) for name in safe_names])\
            .outerjoin( *[(sub_query, sub_query.c.dict_id==d.id)
                            for sub_query in sub_queries] )

    create_view_statement = 'CREATE OR REPLACE VIEW %s AS ' % viewname
    create_view_statement += main_query.statement


    s.close()


