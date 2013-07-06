import sqlalchemy as sa
from sqlalchemy import orm

from sqlalchemy.sql import and_, or_
from sqlalchemy.types import Integer, String
from sqlalchemy.schema import Column, ForeignKey, ForeignKeyConstraint

from tabulator import Tabulator

meta = sa.MetaData()

def init_echo_session(dburi):
    init_and_bind_engine(dburi)
    meta.bind.echo = True
    ses = orm.create_session()
    ses.echo_uow = True
    return ses

def init_and_bind_engine(dburi):
    engine = sa.create_engine(dburi)
    meta.bind = engine
    meta.create_all()

net_table = sa.Table('nets', meta,
        Column('unit', String(20), primary_key=True),
        Column('num', Integer, primary_key=True),
        Column('sig_desc', String(50))
        )

class Net(object):
    def __init__(self, unit, sig_desc=None):
        self.unit = unit
        self.sig_desc = sig_desc

    def __str__(self):
        return "N-%s.%s" % (self.unit, self.num)
    def __repr__(self):
        return "<net %s>" % str(self)
    
class NetMapperExtension(orm.MapperExtension):
    def before_insert(self, mapper, connection, instance):
        num_sel = sa.select(
                [sa.func.max(net_table.c.num).label('max')],
                net_table.c.unit == instance.unit)
        instance.num = num_sel.execute().fetchone().max or 0
        instance.num += 1
    
    def before_delete(self, mapper, connection, instance):
        cdr_mapper = orm.class_mapper(Conductor)
        [ cdr_mapper.delete_obj(cdr) for cdr in instance.linked_cdrs ]

conductor_table = sa.Table('conductors', meta,
        Column('a_net_unit', String(20), nullable=False), #ForeignKey('nets.unit')),
        Column('a_net_num', Integer, nullable=False), #ForeignKey('nets.num')),
        Column('b_net_unit', String(20), nullable=False), #ForeignKey('nets.unit')),
        Column('b_net_num', Integer, nullable=False), #ForeignKey('nets.num')),
        Column('kind', String(20)),
        Column('cable', Integer, primary_key=True),
        Column('subcdr', Integer, primary_key=True),
        ForeignKeyConstraint(['a_net_unit', 'a_net_num'], ['nets.unit', 'nets.num']),
        ForeignKeyConstraint(['b_net_unit', 'b_net_num'], ['nets.unit', 'nets.num'])
        )

class Conductor(object):
    def __init__(self, a_net, b_net, kind='C', cable=None):
        self.a_net = a_net
        self.b_net = b_net
        self.kind = kind
        self.cable = cable

    def __str__(self):
        if self.subcdr and self.kind == 'C':
            return "C-%s.%s" % (self.cable, self.subcdr)
        elif self.subcdr:
            return "C-%s.%s (%s)" % (self.cable, self.subcdr, self.kind)
        elif self.kind == 'C':
            return "C-%s" % (self.cable)
        else:
            return "C-%s (%s)" % (self.cable, self.kind)
    def __repr__(self):
        return "<conductor %s>" % str(self)

    def swap_orientation(self):
        self.a_net, self.b_net = self.b_net, self.a_net

class ConductorMapperExtension(orm.MapperExtension):
    def before_insert(self, mapper, connection, instance):

        if instance.cable == None:
            cable_sel = sa.select(
                    [sa.func.max(conductor_table.c.cable).label('max')])
            instance.cable = cable_sel.execute().fetchone().max or 0
            instance.cable += 1

        subcdr_sel = sa.select(
                [sa.func.max(conductor_table.c.subcdr).label('max')],
                conductor_table.c.cable == instance.cable)
        instance.subcdr = subcdr_sel.execute().fetchone().max or 0
        instance.subcdr += 1

    def after_delete(self, mapper, connection, instance):
        conductor_table.update(
                and_(conductor_table.c.cable==instance.cable,
                    conductor_table.c.subcdr > instance.subcdr),
                values={ conductor_table.c.subcdr: 
                    conductor_table.c.subcdr - 1 }).execute()

pin_table = sa.Table('pins', meta,
        Column('net_unit', String(20), nullable=False, #ForeignKey('nets.unit'),
            primary_key=True),
        Column('net_num', Integer, nullable=False, #ForeignKey('nets.num'),
            primary_key=True),
        Column('conn', String(20), primary_key=True),
        Column('desig', String(20), primary_key=True),
        Column('sig_desc', String(50)),
        ForeignKeyConstraint(['net_unit', 'net_num'], ['nets.unit', 'nets.num'])
        )

class Pin(object):
    def __init__(self, conn, desig, net=None, sig_desc=None):
        self.net = net
        self.conn = conn
        self.desig = desig
        self.sig_desc = sig_desc

    def __str__(self):
        return "P-%s.%s.%s (%s)" % (self.net and self.net.unit or None, 
                self.conn, self.desig, self.sig_desc)
    def __repr__(self):
        return "<pin %s>" % str(self)

net_to_cdr_join = or_(
    and_(net_table.c.unit == conductor_table.c.a_net_unit,
        net_table.c.num == conductor_table.c.a_net_num),
    and_(net_table.c.unit == conductor_table.c.b_net_unit,
        net_table.c.num == conductor_table.c.b_net_num))

orm.mapper(Net, net_table, properties = {
    'linked_cdrs': orm.relation(Conductor,
        primaryjoin = net_to_cdr_join,
#        foreign_keys = [
#            conductor_table.c.a_net_unit,
#            conductor_table.c.a_net_num,
#            conductor_table.c.b_net_unit,
#            conductor_table.c.b_net_num],
        order_by = [ 
            conductor_table.c.cable,
            conductor_table.c.subcdr ]
        ),
    'linked_pins': orm.relation(Pin,
#        primaryjoin = and_(
#            net_table.c.unit == pin_table.c.net_unit,
#            net_table.c.num == pin_table.c.net_num),
#        foreign_keys = [
#            pin_table.c.net_unit,
#            pin_table.c.net_num],
        cascade = 'all, delete-orphan',
        backref = orm.backref('net', lazy=False)
        )
    }, extension=NetMapperExtension())

orm.mapper(Conductor, conductor_table, properties = {
    'a_net': orm.relation(Net,
        primaryjoin = and_(
            conductor_table.c.a_net_unit == net_table.c.unit,
            conductor_table.c.a_net_num == net_table.c.num)
        ),
    'b_net': orm.relation(Net,
        primaryjoin = and_(
            conductor_table.c.b_net_unit == net_table.c.unit,
            conductor_table.c.b_net_num == net_table.c.num)
        )
    }, extension=ConductorMapperExtension())

pin_net_join = and_(
        pin_table.c.net_unit == net_table.c.unit,
        pin_table.c.net_num == net_table.c.num)

orm.mapper(Pin, pin_table) #, properties = {
#    'net': orm.relation(Net, uselist=False) #, primaryjoin=pin_net_join)
#    })




class Interconnect(object):

    def __init__(self, dburi='sqlite://', debug=True):
        init_and_bind_engine(dburi)
        self._debug = debug
        self._ses = None
        self._filter_units = None
        self._link_filter = False
        
        if debug:
            meta.bind.echo = True

    def _get_ses(self):
        if not self._ses:
            self._ses = orm.create_session()
            self._ses.echo_uow = self._debug
        return self._ses

    def _close_ses(self):
        if self._ses:
            self._ses.close()
            self._ses = None

    def _pin_table_query(self):
        s = self._get_ses()

        pt_q = s.query(Pin).add_entity(Net).outerjoin('net')

        if self._link_filter:
            pt_q = pt_q.outerjoin('net', 'linked_cdrs').filter(
                    and_(conductor_table.c.a_net_unit.in_(self._filter_units),
                        conductor_table.c.b_net_unit.in_(self._filter_units)))

        elif self._filter_units:
            pt_q = pt_q.filter(
                    net_table.c.unit.in_(self._filter_units))

        pt_q = pt_q.order_by(net_table.c.unit, net_table.c.num,
                pin_table.c.conn, pin_table.c.desig)

        return pt_q

    def _cdr_table_query(self):
        s = self._get_ses()

        a_pins = orm.aliased(Pin)
        b_pins = orm.aliased(Pin)
        ct_q = s.query(Conductor, a_pins, b_pins).filter(
                a_pins.net_unit==Conductor.a_net_unit,
                a_pins.net_num==Conductor.a_net_num,
                b_pins.net_unit==Conductor.b_net_unit,
                b_pins.net_num==Conductor.b_net_num)

        if self._filter_units:
            oper = and_ if self._link_filter else or_
            ct_q = ct_q.filter(oper(
                    a_pins.net_unit.in_(self._filter_units),
                    b_pins.net_unit.in_(self._filter_units)))

        int_ = lambda exp: sa.cast(exp, sa.Integer)
        ct_q = ct_q.order_by(
                a_pins.net_unit, int_(a_pins.conn), a_pins.conn,
                int_(a_pins.desig), a_pins.desig,
                b_pins.net_unit, int_(b_pins.conn), b_pins.conn,
                int_(b_pins.desig), b_pins.desig)

        return ct_q

    @property
    def unit_display_filter(self):
        return self._filter_units
    @unit_display_filter.setter
    def unit_display_filter(self, value):
        self._filter_units = value

    def toggle_link_filter(self):
        self._link_filter = not self._link_filter
        return self._link_filter

    def conn_tables(self, file, show_pcids=True):

        if self._link_filter:
            print "*** Link Filter is ON ***"

        pin_tables_q = self._pin_table_query()
        pin_tables_result = list(pin_tables_q)

        output_lines = [ ['Unit', 'Net', 'Conn', 'Pin',
            'Sig Desc', 'Cdrs', 'Ref Net', 'PCID'], ]

        prev_output_line = [None] * 7
        pcid = 0
        for index in range(len(pin_tables_result)):
            cur_pin, cur_net = pin_tables_result[index]
            
            unblanked_output_line = [
                    cur_net.unit,
                    ('.%s' % str(cur_net.num)),
                    cur_pin.conn,
                    cur_pin.desig,
                    cur_pin.sig_desc or '',
                    '', '', pcid ]
            output_line = list(unblanked_output_line)
            pcid += 1

            for field in range(5):
                if output_line[field] == prev_output_line[field]:
                    output_line[field] = ''
                elif field != 1:
                    break
            
            prev_output_line = unblanked_output_line

            if len(output_line[0]):
                output_lines.append([''] * 7)

            output_lines.append(output_line)

            try:
                if cur_net == pin_tables_result[index + 1][1]:
                    continue
            except IndexError:
                pass

            for cdr in cur_net.linked_cdrs:
                output_line[5] = cdr

                if cur_net == cdr.a_net:
                    output_line[6] = cdr.b_net
                else:
                    output_line[6] = cdr.a_net

                output_line = [''] * 7
                output_lines.append(output_line)

            if cur_net.linked_cdrs:
                del output_lines[-1]

        if show_pcids:
            t = Tabulator(10, 5, 6, 4, 20, 12, 16, 4)
        else:
            t = Tabulator(10, 5, 6, 4, 20, 12, 16, 0)

        for line in output_lines:
            file.write(t(*line) + '\n')

    def cdr_table(self, file):

        if self._link_filter:
            print "*** Link Filter is ON ***"

        cdr_table_q = self._cdr_table_query()
        cdr_table_result = list(cdr_table_q)

        output_lines = [ ['Unit', 'Conn', 'Pin', '[/]', 'Cdr', '[/]',
            'Pin', 'Conn', 'Unit'], [''] * 9 ]

        prev_output_line = [None] * 9

        for index in range(len(cdr_table_result)):
            cur_cdr, cur_a_pin, cur_b_pin = cdr_table_result[index]

            unblanked_output_line = [
                    cur_a_pin.net_unit,
                    cur_a_pin.conn,
                    cur_a_pin.desig,
                    '[ ]',
                    str(cur_cdr),
                    '[ ]',
                    cur_b_pin.desig,
                    cur_b_pin.conn,
                    cur_b_pin.net_unit ]
            output_line = list(unblanked_output_line)

            for fieldset in [[4], [0, 1, 2, 3], [8, 7, 6, 5]]:
                for field in fieldset:
                    if output_line[field] == prev_output_line[field]:
                        output_line[field] = ''
                    else:
                        break

            prev_output_line = unblanked_output_line
            output_lines.append(output_line)

        t = Tabulator(12, 6, 4, 5, 6, 5, 4, 6, 12)
        for line in output_lines:
            file.write(t(*line) + '\n')

    def add_cdr(self, a_unit, a_conn, a_pin,
            b_unit, b_conn, b_pin, cable=None):
        s = self._get_ses()

        def find_or_create_pin_net(unit, conn, pin):
            q = s.query(Pin).add_entity(Net).join('net') \
                    .filter(net_table.c.unit == unit) \
                    .filter(pin_table.c.conn == conn) \
                    .filter(pin_table.c.desig == pin)
            if q.count():
                the_net = list(q)[0][1]
                print " - pin %s/%s/%s exists on net %s" % (
                        unit, conn, pin, repr(the_net))
                return the_net
            else:
                the_net = Net(unit)
                the_pin = Pin(conn, pin)
                the_net.linked_pins.append(the_pin)
                print " - creating pin %s/%s/%s, net %s" % (
                        unit, conn, pin, repr(the_net))
                s.add(the_net)
                return the_net

        pin_a_net = find_or_create_pin_net(a_unit, a_conn, a_pin)
        pin_b_net = find_or_create_pin_net(b_unit, b_conn, b_pin)
        new_cdr = Conductor(pin_a_net, pin_b_net, cable=cable)

        s.add(new_cdr)
        s.flush()

        print pin_a_net
        print pin_b_net
        print new_cdr
        
        self._close_ses()

    def add_pin(self, unit, conn, pin, net_num):
        s = self._get_ses()

        existing_net = s.query(Net).filter(net_table.c.unit == unit) \
                .filter(net_table.c.num == net_num)[0]
        
        the_pin = Pin(conn, pin)
        existing_net.linked_pins.append(the_pin)
        s.flush()

        self._close_ses()

    def del_cdr(self, cable, subcdr):
        self._close_ses()
        conductor_table.delete(and_(conductor_table.c.cable == cable,
                conductor_table.c.subcdr == subcdr)).execute()

    def desc_pin(self, pin_cid, desc):
        pin = self._pin_table_query()[pin_cid][0]
        pin.sig_desc = desc
        self._get_ses().flush()
        self._close_ses()

    def del_pin(self, pin_cid):
        pin = self._pin_table_query()[pin_cid][0]
        s = self._get_ses()
        s.delete(pin)
        s.flush()
        self._close_ses()

    def rename_pin(self, pin_cid, conn, desig):
        pin = self._pin_table_query()[pin_cid][0]
        pin.conn = conn
        pin.desig = desig
        self._get_ses().flush()
        self._close_ses()

    def del_net(self, unit, net_num):
        s = self._get_ses()
        net = s.query(Net).get([unit, net_num])
        s.delete(net)
        s.flush()
        self._close_ses()

    def complete_unit(self, prefix):
        compl_r = sa.select([net_table.c.unit],
            whereclause=net_table.c.unit.like(prefix + '%'),
            order_by=[net_table.c.unit], distinct=True).execute()
        return [ r[0] + ' ' for r in compl_r ]

    def complete_conn(self, unit, prefix):
        pn_j = sa.join(pin_table, net_table, onclause=pin_net_join)
        compl_r = sa.select([pin_table.c.conn],
            whereclause=and_(
                net_table.c.unit==unit,
                pin_table.c.conn.like(prefix +'%')
                ),
            from_obj=[pn_j],
            order_by=[pin_table.c.conn], distinct=True).execute()
        return [ r[0] + ' ' for r in compl_r ]

    def renumber_nets(self, unit):
        conn = meta.bind.connect()
        trans = conn.begin()
        try:

            nets_sorted_j = sa.outerjoin(net_table, pin_table,
                    onclause = pin_net_join)
            nets_sorted_q = sa.select(
                    [net_table.c.unit, net_table.c.num,
                        sa.func.min(pin_table.c.conn)
                            .label('lexical_conn_sort'),
                        sa.func.min(sa.cast(pin_table.c.conn, sa.Integer))
                            .label('numeric_conn_sort'),
                        sa.func.min(pin_table.c.desig)
                            .label('lexical_pin_sort'),
                        sa.func.min(sa.cast(pin_table.c.desig, sa.Integer))
                            .label('numeric_pin_sort')],
                    from_obj = [nets_sorted_j],
                    whereclause = net_table.c.unit==unit,
                    group_by = [net_table.c.unit, net_table.c.num],
                    order_by = [net_table.c.unit,
                        'numeric_conn_sort', 'lexical_conn_sort',
                        'numeric_pin_sort', 'lexical_pin_sort'])
            nets_sorted = list(conn.execute(nets_sorted_q))
            

            for index in range(len(nets_sorted)):
                new_num = index + 1
                print nets_sorted[index], '->', new_num

                # This is a marginal hack to avoid having to do more magic
                # to keep from breaking PK uniqueness constraints.
                conn.execute(net_table.update(and_(
                    net_table.c.unit==nets_sorted[index].unit,
                    net_table.c.num==nets_sorted[index].num),
                    { 'num': -new_num }))
                conn.execute(pin_table.update(and_(
                    pin_table.c.net_unit==nets_sorted[index].unit,
                    pin_table.c.net_num==nets_sorted[index].num),
                    { 'net_num': -new_num }))
                conn.execute(conductor_table.update(and_(
                    conductor_table.c.a_net_unit==nets_sorted[index].unit,
                    conductor_table.c.a_net_num==nets_sorted[index].num),
                    { 'a_net_num': -new_num }))
                conn.execute(conductor_table.update(and_(
                    conductor_table.c.b_net_unit==nets_sorted[index].unit,
                    conductor_table.c.b_net_num==nets_sorted[index].num),
                    { 'b_net_num': -new_num }))

            conn.execute(net_table.update(net_table.c.num<0,
                    { 'num': sa.func.abs(net_table.c.num) }))
            conn.execute(pin_table.update(pin_table.c.net_num<0,
                    { 'net_num': sa.func.abs(pin_table.c.net_num) }))
            conn.execute(conductor_table.update(conductor_table.c.a_net_num<0,
                    { 'a_net_num': sa.func.abs(conductor_table.c.a_net_num) }
                    ))
            conn.execute(conductor_table.update(conductor_table.c.b_net_num<0,
                    { 'b_net_num': sa.func.abs(conductor_table.c.b_net_num) }
                    ))

            trans.commit()
        except:
            trans.rollback()
            raise
        finally:
            conn.close()
            self._close_ses()

    def reorient_cdrs(self):
        conn = meta.bind.connect()
        try:

            net_cdr_j = sa.outerjoin(net_table, conductor_table,
                    onclause=net_to_cdr_join)
            unit_fan_order_q = sa.select(
                    [net_table.c.unit,
                        sa.func.count(conductor_table.c.cable).label('cdr_count')],
                    from_obj = [net_cdr_j],
                    group_by = [net_table.c.unit],
                    order_by = [sa.asc('cdr_count'), net_table.c.unit])
            unit_fan_order = [r[0] for r in conn.execute(unit_fan_order_q)]
        finally:
            conn.close()

        s = self._get_ses()
        
        s.begin()
        try:
            for unit in unit_fan_order:
                b_cdrs = s.query(Conductor).options(
                        orm.joinedload('a_net'),
                        orm.joinedload('b_net')).filter(Conductor.b_net_unit==unit)
                for cdr in b_cdrs:
                    cdr.swap_orientation()
                s.flush()
            s.commit()
        except:
            s.rollback()
            raise
        finally:
            self._close_ses()

    def set_cdr_kind(self, cable, subcdr, new_kind):
        s = self._get_ses()
        cdr = s.query(Conductor).get([cable, subcdr])
        cdr.kind = new_kind
        s.update(cdr)
        s.flush()
        self._close_ses()

    def set_cdr_sub(self, cable, subcdr, new_subcdr):
        self._close_ses()
        cdrs = conductor_table.select(and_(conductor_table.c.cable == cable,
            conductor_table.c.subcdr == new_subcdr)).count().execute()

        if list(cdrs)[0][0]:
            raise RuntimeError(
                'The cable has a conductor with that ID already.')
        
        conductor_table.update(values = 
                { conductor_table.c.subcdr: new_subcdr },
                whereclause = and_(
                    conductor_table.c.subcdr == subcdr,
                    conductor_table.c.cable == cable)
                ).execute()
