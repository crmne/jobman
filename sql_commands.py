

def crazy_sql_command(tablename, viewname, cols, 
        id_col='id', name_col='name', val_col='value'):


    #create or replace view expview as select * from (select id as v1_id, value as nhid from
    #test0 where name='nhid') nhid LEFT OUTER JOIN (select id as v2_id, value as lrate from
    #test0 where name='lrate') lrate on nhid.v1_id = lrate.v2_id;

    header = " create or replace view %s as select %s from " \
            % (viewname, (", ".join([c[0] for c in cols])))

    col_queries = []
    colname0 = None
    for i, (colname, table_col) in enumerate(cols):
        if i == 0:
            q = """(select %(id_col)s as v%(i)s_id, %(table_col)s as %(colname)s 
                    from %(tablename)s 
                    where name='%(colname)s') %(colname)s """ % locals()
            colname0 = colname
        else:
            q = """ LEFT OUTER JOIN (select %(id_col)s as v%(i)s_id, %(table_col)s as %(colname)s 
                    from %(tablename)s 
                    where name='%(colname)s')
                    %(colname)s 
                    on %(colname0)s.v0_id = %(colname)s.v%(i)s_id""" % locals()
        col_queries.append(q)

    rval = header + "\n".join(col_queries)

    return rval


print crazy_sql_command('test0', 'expview', (('nhid', 'value'), ('lrate', 'value'), ('a',
    'value')))

