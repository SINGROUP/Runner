"""cli for runner

Formatter, and argparse ques taken from ASE

ASE is free software: you can redistribute it and/or modify
it under the terms of the GNU Lesser General Public License as published by
the Free Software Foundation, either version 2.1 of the License, or
(at your option) any later version.
"""
import argparse
import textwrap
from ase.db import connect
from ase.io.formats import string2index
from runner.runners.__init__ import runner_type2func
from runner.utils import submit, cancel, get_status


def main(prog='runner', args=None):
    """main function for cli
    Args:
        prog (str): Name of the program
        args (list): list of args"""
    cmd = CLICommand
    docstring = cmd.__doc__

    parser = argparse.ArgumentParser(prog=prog,
                                     description=docstring,
                                     formatter_class=Formatter)
    cmd.add_arguments(parser)

    # run
    args = parser.parse_args(args)
    cmd.run(args)


class CLICommand:
    """Tools to easly schedule atomistic simulation workflow.
    """

    @staticmethod
    def add_arguments(parser):
        """adds runner arguments to parser"""
        parent = argparse.ArgumentParser(description='database parser',
                                         add_help=False)
        parent.add_argument('-db', metavar='database-name', required=True,
                            help='SQLite3 file, JSON file or postgres URL.')

        subparsers = parser.add_subparsers(title='Sub-commands',
                                           description='required subcommands',
                                           dest='action',
                                           required=True)

        # list runners and their status
        desc = 'list runners and their status'
        subparser = subparsers.add_parser('list-runners',
                                          formatter_class=Formatter,
                                          parents=[parent],
                                          description=desc,
                                          help=desc)

        # remove runner
        desc = 'remove runner from metadata'
        subparser = subparsers.add_parser('remove-runner',
                                          formatter_class=Formatter,
                                          parents=[parent],
                                          description=desc,
                                          help=desc)
        subparser.add_argument('name', help='name of runner to remove')

        # start runner
        desc = 'start a runner from metadata'
        subparser = subparsers.add_parser('start',
                                          formatter_class=Formatter,
                                          parents=[parent],
                                          description=desc,
                                          help=desc)

        subparser.add_argument('name', help='name of the runner to start')

        # submit a row
        desc = ("submit row(s) for run.")
        add = ("Row ids can be int or python like"
               " slice, eg. '1:4' gives ids 1, 2, and 3")
        subparser = subparsers.add_parser('submit',
                                          formatter_class=Formatter,
                                          parents=[parent],
                                          description=desc+'\n'+add,
                                          help=desc)

        subparser.add_argument('name', help='name of runner to submit on')

        group = subparser.add_mutually_exclusive_group(required=True)
        group.add_argument('-c', '--cancelled', action='store_true',
                           help='submit all cancelled')
        group.add_argument('-f', '--failed', action='store_true',
                           help='submit all failed')
        group.add_argument('-id', type=string2index,
                           help='id of the row in the database')

        # cancel a submitted row
        desc = ("cancel row(s) for run.")
        add = ("Row ids can be int or python like"
               " slice, eg. '1:4' gives ids 1, 2, and 3")
        subparser = subparsers.add_parser('cancel',
                                          formatter_class=Formatter,
                                          parents=[parent],
                                          description=desc+'\n'+add,
                                          help=desc)

        subparser.add_argument('name', help='name of runner to submit on')

        group = subparser.add_mutually_exclusive_group(required=True)
        group.add_argument('-a', '--all', action='store_true',
                           help='cancel all submitted and running')
        group.add_argument('-s', '--submitted', action='store_true',
                           help='cancel all submitted')
        group.add_argument('-r', '--running', action='store_true',
                           help='cancel all running')
        group.add_argument('-id', type=string2index,
                           help='id of the row in the database')

        # check status of a row
        desc = 'check running status of row(s)'
        add = ("Row ids can be int or python like"
               " slice, eg. '1:4' gives ids 1, 2, and 3")
        subparser = subparsers.add_parser('status',
                                          formatter_class=Formatter,
                                          parents=[parent],
                                          description=desc+'\n'+add,
                                          help=desc)
        subparser.add_argument('id', type=string2index,
                               help='id of the row in the database')

    @staticmethod
    def run(args):
        """run cli args"""
        with connect(args.db) as fdb:

            if args.action == 'list-runners':
                runner_meta = fdb.metadata.get('runners', {})
                count = 1
                # header
                print(' '*22 + 'Runner name' + ' '*25 + 'Status', '\n', '='*63)
                for key, value in runner_meta.items():
                    bool_ = ('running' if value.get('running', False)
                             else 'not running')
                    print('{:>2} {:>30} {:>30}'.format(count, key, bool_))
                    count += 1
            elif args.action == 'remove-runner':
                raise NotImplementedError()
            elif args.action == 'start':
                name = args.name
                func = runner_type2func[name.split(':')[0]]
                runner = func.from_database(name, args.db)
                runner.spool()
            elif args.action == 'submit':
                if args.cancelled:
                    args.id = []
                    for row in fdb.select(status=f'cancel:{args.name}'):
                        args.id.append(row.id)
                elif args.failed:
                    args.id = []
                    for row in fdb.select(status=f'failed:{args.name}'):
                        args.id.append(row.id)

                if isinstance(args.id, list):
                    for id_ in args.id:
                        submit(id_, args.db, args.name)
                elif isinstance(args.id, slice):
                    for id_ in range(args.id.start or 1,
                                     args.id.stop or fdb.count()+1,
                                     args.id.step or 1):
                        submit(id_, args.db, args.name)
                else:
                    submit(args.id, args.db, args.name)
            elif args.action == 'cancel':
                if args.submitted or args.running or args.all:
                    args.id = []
                if args.submitted or args.all:
                    for row in fdb.select(status=f'submit:{args.name}'):
                        args.id.append(row.id)
                if args.running or args.all:
                    for row in fdb.select(status=f'failed:{args.name}'):
                        args.id.append(row.id)

                if isinstance(args.id, list):
                    for id_ in args.id:
                        cancel(id_, args.db)
                elif isinstance(args.id, slice):
                    for id_ in range(args.id.start or 1,
                                     args.id.stop or fdb.count()+1,
                                     args.id.step or 1):
                        cancel(id_, args.db)
                else:
                    cancel(args.id, args.db)
            elif args.action == 'status':
                if isinstance(args.id, int):
                    args.id = slice(args.id, args.id+1)
                # header
                print(' '*2 + 'ID' + ' '*25 + 'Status', '\n', '='*34)
                for id_ in range(args.id.start or 1,
                                 args.id.stop or fdb.count()+1,
                                 args.id.step or 1):
                    print(f'{id_:>4} {get_status(id_, args.db):>30}')


class Formatter(argparse.HelpFormatter):
    """Improved help formatter."""

    def _fill_text(self, text, width, indent):
        # assert indent == ''
        out = ''
        blocks = text.split('\n\n')
        for block in blocks:
            if block[0] == '*':
                # List items:
                for item in block[2:].split('\n* '):
                    out += textwrap.fill(item,
                                         width=width - 2,
                                         initial_indent='* ',
                                         subsequent_indent='  ') + '\n'
            elif block[0] == ' ':
                # Indented literal block:
                out += block + '\n'
            else:
                # Block of text:
                out += textwrap.fill(block, width=width) + '\n'
            out += '\n'
        return out[:-1]


if __name__ == "__main__":
    main()
