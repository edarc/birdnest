#!/usr/bin/python

import sys, cmd
import saobjects as sao
import readline

class Birdnest(cmd.Cmd):

    def __init__(self):
        cmd.Cmd.__init__(self)
        self.prompt = "BN> "
        self.ic = None
        default_delims = readline.get_completer_delims()
        new_delims = ''.join([ (c not in '-/[]') and c or '' for c in default_delims ])
        readline.set_completer_delims(new_delims)

    def _required_args(self, args, required):
        if len(args) not in required:
            print '*** command requires %s args (%s given)' \
                    % (', '.join([str(r) for r in required]),
                        str(len(args)))
            return True

    def emptyline(self):
        self.ic.conn_tables(sys.stdout)
    def do_emit(self, s):
        output_file = file(s, 'w')
        self.ic.conn_tables(output_file, show_pcids=False)
        output_file.close()
    def help_exit(self):
        print """\
            exit
            
            Exit Birdnest
            """
    def do_exit(self, s):
        return True
    do_EOF = do_exit

    def do_massemit(self, s):
        units = self.ic.complete_unit('')
        
        for unit in units:
            output_file = file("%s%s.il" % (s, unit.strip().lower()), 'w')
            self.ic.unit_display_filter = [unit.strip()]
            self.ic.conn_tables(output_file, show_pcids=False)
            output_file.close()

    def do_cdrcheck(self, s):
        self.ic.cdr_table(sys.stdout)

    def help_bind(self):
        print """\
            bind <db-uri>

            Bind to an interconnect database specified by it's
            SQLAlchemy URI.
            """
    def do_bind(self, s):
        self.ic = sao.Interconnect(s, debug=False)

    def help_filterunits(self):
        print """\
            filterunits [+|-] [<unit>, ...]

            Filter out all but the listed units in the pin table display. If +
            or - precedes the listed units, adds or removes them from the set
            of units to be displayed.  Specifying no units removes the filter
            (all units are shown).
            """
    def do_filterunits(self, s):
        args = s.split()

        if not len(args):
            self.ic.unit_display_filter = None
        elif args[0] == '+':
            self.ic.unit_display_filter = list(
                    set((self.ic.unit_display_filter or []))
                    .union(set(args[1:])))
        elif args[0] == '-':
            if not self.ic.unit_display_filter:
                self.ic.unit_display_filter = [
                        unit.strip() for unit in self.ic.complete_unit('')]
            self.ic.unit_display_filter = list(
                    set(self.ic.unit_display_filter)
                    - set(args[1:]))
        else:
            self.ic.unit_display_filter = args
        print ' '.join(sorted(self.ic.unit_display_filter or []))

    def complete_filterunits(self, text, line, begidx, endidx):
        current_args = line.split()[1:]
        arg_num = len((line + 'X').split()) - 2

        return self.ic.complete_unit(text)

    def help_linkfilter(self):
        print """\
            linkfilter

            Toggle filtering of individual nets by whether or not they have
            conductors which link the units enabled with the filterunits
            command.
            """
    def do_linkfilter(self, s):
        print "Link Filter is now %s" % (self.ic.toggle_link_filter() and 'ON' or 'OFF')

    def help_ac(self):
        print """\
            ac <a-unit> <a-conn> <a-pin-desig> <b-unit> <b-conn>
               <b-pin-desig> [<cable>]

            Add a conductor (optionally in an existing cable <cable>) from
            <a-pin-desig> in connector <a-conn> on unit <a-unit> to
            <b-pin-desig> in <b-conn> on <b-pin>. If a pin does not exist,
            it will be created and linked to the conductor, otherwise the
            conductor will be linked into the pin's current net.
            """
    def do_ac(self, s):
        args = s.split()

        if self._required_args(args, [6, 7]):
            return

        try:
            self.ic.add_cdr(*args)
        except Exception, exc:
            print '***', exc

    def complete_ac(self, text, line, begidx, endidx):
        current_args = line.split()[1:]
        arg_num = len((line + 'X').split()) - 2

        if arg_num in [0, 3]:
            return self.ic.complete_unit(text)
        elif arg_num == 1:
            return self.ic.complete_conn(current_args[0], text)
        elif arg_num == 4:
            return self.ic.complete_conn(current_args[3], text)

    def help_ap(self):
        print """\
            ap <unit> <conn> <pin-desig> <net-num>

            Adds pin <pin-desig> in connector <conn> on unit <unit> to an
            existing net numbered <net-num> on the same unit. This net must
            exist or the operation will fail.
            """
    def do_ap(self, s):
        args = s.split()

        if self._required_args(args, [4]):
            return

        try:
            args[3] = int(args[3])
            self.ic.add_pin(*args)
        except Exception, exc:
            print '***', exc

    def complete_ap(self, text, line, begidx, endidx):
        current_args = line.split()[1:]
        arg_num = len((line + 'X').split()) - 2

        if arg_num == 0:
            return self.ic.complete_unit(text)
        elif arg_num == 1:
            return self.ic.complete_conn(current_args[0], text)

    def help_rmcdr(self):
        print """\
            rmcdr <cable> <subcdr>

            Deletes specified conductor
            """
    def do_rmcdr(self, s):
        args = s.split()

        if self._required_args(args, [2]):
            return

        try:
            for i in range(2):
                args[i] = int(args[i])
            self.ic.del_cdr(*args)
        except Exception, exc:
            print '***', exc

    def help_descp(self):
        print """\
            descp <pin-cid> <sig-desc...>

            Alter the signal description to <sig-desc...> for the pin
            designated by <pin-cid>.
            """
    def do_descp(self, s):
        args = s.split(None, 1)

        if self._required_args(args, [2]):
            return

#        try:
        args[0] = int(args[0])
        self.ic.desc_pin(*args)
#        except Exception, exc:
#            print '***', exc

    def help_rmnet(self):
        print """\
            rmnet <unit> <net-num>

            Deletes the net specified and all conductors and pins linked to
            it.
            """
    def do_rmnet(self, s):
        args = s.split()

        if self._required_args(args, [2]):
            return

        try:
            args[1] = int(args[1])
            self.ic.del_net(*args)
        except Exception, exc:
            print '***', exc

    def complete_rmnet(self, text, line, begidx, endidx):
        current_args = line.split()[1:]
        arg_num = len((line + 'X').split()) - 2

        if arg_num == 0:
            return self.ic.complete_unit(text)

    def help_rmpin(self):
        print """\
            rmpin <pin-cid>

            Deletes the pin designated by <pin-cid>. If this pin is the
            only member of it's net, the net will become inaccessibly
            orphaned (use rmnet instead).
            """
    def do_rmpin(self, s):
        try:
            arg = int(s)
            self.ic.del_pin(arg)
        except Exception, exc:

            print '***', exc

    def help_chpin(self):
        print """\
            chpin <pin-cid> <conn> <desig>

            Change the connector and position designation on the pin identified
            by <pin-cid>.
            """
    def do_chpin(self, s):
        args = s.split()

        if self._required_args(args, [3]):
            return

        try:
            args[0] = int(args[0])
            self.ic.rename_pin(*args)
        except Exception, exc:
            print '***', exc

    def help_renumbernets(self):
        print """\
            renumbernets <unit>

            Renumbers the nets attached to unit <unit> to better group
            connectors together and to try to order the pins
            numerically/alphabetically.
            """
    def do_renumbernets(self, s):
        args = s.split()

        if self._required_args(args, [1]):
            return

        self.ic.renumber_nets(args[0])

    def complete_renumbernets(self, text, line, begidx, endidx):
        current_args = line.split()[1:]
        arg_num = len((line + 'X').split()) - 2

        if arg_num == 0:
            return self.ic.complete_unit(text)

    def do_reorientcdrs(self, s):
        self.ic.reorient_cdrs()

    def help_setsub(self):
        print """\
            setsub <cable> <subcdr> <newsubcdr>

            Sets the conductor <subcdr> in <cable> to have a subconductor
            ID of <newsubcdr>. For shields and non-cable conductors, this
            can be set to zero and the subconductor ID will not be
            displayed.
            """
    def do_setsub(self, s):
        args = s.split()

        if self._required_args(args, [3]):
            return

        try:
            if len(args) == 2:
                args.append(0)

            for i in range(3):
                args[i] = int(args[i])

            self.ic.set_cdr_sub(*args)
        except Exception, exc:
            print '***', exc

    def help_setkind(self):
        print """\
            setkind <cable> <subcdr> <newkind>

            sets the conductor <subcdr> in <cable> to have kind
            <newkind>. If the kind is C (normal conductor), a specific
            kind will not be displayed in output tables.
            """

    def do_setkind(self, s):
        args = s.split()

        if self._required_args(args, [3]):
            return

        try:
            for i in range(2):
                args[i] = int(args[i])

            self.ic.set_cdr_kind(*args)
        except Exception, exc:
            print '***', exc


if __name__ == '__main__':
    bncl = Birdnest()
    bncl.cmdloop()
