
def crazy_sql_command(viewname, cols, keytab, linktab, id_col='id', dict_id='dict_id', pair_id='pair_id'):

    #create or replace view expview as select * from (select id as v1_id, value as nhid from
    #test0 where name='nhid') nhid LEFT OUTER JOIN (select id as v2_id, value as lrate from
    #test0 where name='lrate') lrate on nhid.v1_id = lrate.v2_id;

    col_queries = []
    colname0 = None
    for i, (colname, table_col) in enumerate(cols):
        if i == 0:
            q = """(select %(dict_id)s as v%(i)s_id, %(table_col)s as %(colname)s 
                    from \"%(keytab)s\", \"%(linktab)s\"
                    where name='%(colname)s'
                    and \"%(keytab)s\".%(id_col)s = \"%(linktab)s\".%(pair_id)s)
                    %(colname)s """ % locals()
            colname0 = colname
        else:
            q = """ LEFT OUTER JOIN (select %(dict_id)s as v%(i)s_id, %(table_col)s as %(colname)s 
                    from \"%(keytab)s\", \"%(linktab)s\" 
                    where name='%(colname)s'
                    and  \"%(keytab)s\".%(id_col)s = \"%(linktab)s\".%(pair_id)s)
                    %(colname)s 
                    on %(colname0)s.v0_id = %(colname)s.v%(i)s_id""" % locals()
        col_queries.append(q)

    header = " create or replace view %s as select %s.v0_id as id, %s from " \
            % (viewname, colname0, (", ".join([c[0] for c in cols])))

    rval = header + "\n".join(col_queries)

    return rval


#print crazy_sql_command('test0', 'expview', (('nhid', 'value'), ('lrate', 'value'), ('a','value')))

