from ConfigParser import ConfigParser, DEFAULTSECT
import json
import re


class JsonConfig(dict):
    def __init__(self, filename):
        self.update(json.load(open(filename)))


class EllipticsConfigParser(ConfigParser):

    SECTCRE = re.compile(
        r'\s*<(?P<closing>\/)?'               # <
        r'(?P<header>[^>]+)'                  # very permissive!
        r'>'                                  # >
        )

    OPTCRE = re.compile(
        r'\s*(?P<option>[^:=\s][^:=]*)'       # very permissive!
        r'\s*(?P<vi>[:=])\s*'                 # any number of space/tab,
                                              # followed by separator
                                              # (either : or =), followed
                                              # by any # space/tab
        r'(?P<value>.*)$'                     # everything up to eol
        )

    def _read(self, fp, fpname):
        """Parse a sectioned setup file.

        The sections in setup file contains a title line at the top,
        indicated by a name in square brackets (`[]'), plus key/value
        options lines, indicated by `name: value' format lines.
        Continuations are represented by an embedded newline then
        leading whitespace.  Blank lines, lines beginning with a '#',
        and just about everything else are ignored.
        """
        cursect = None                            # None, or a dictionary
        cursects = list()                         # None, or a dictionary
        optname = None
        lineno = 0
        e = None                                  # None, or an exception
        while True:
            line = fp.readline()
            if not line:
                break
            lineno = lineno + 1
            # comment or blank line?
            if line.strip() == '' or line.strip()[0] in '#;':
                continue
            # a section header or option header?
            else:
                # is it a section header?
                mo = self.SECTCRE.match(line)
                if mo:
                    if cursects:
                        nameslist = [cursects[-1]['__name__']]
                    else:
                        nameslist = []

                    if not mo.group('closing'):
                        nameslist.append(mo.group('header'))

                    sectname = '/'.join(nameslist)

                    if mo.group('closing'):
                        cursects.pop()
                        continue

                    if sectname in self._sections:
                        cursects.append(self._sections[sectname])
                    elif sectname == DEFAULTSECT:
                        cursects.append(self._defaults)
                    else:
                        cursect = self._dict()
                        cursect['__name__'] = sectname
                        self._sections[sectname] = cursect
                        cursects.append(cursect)
                    # So sections can't start with a continuation line
                    optname = None
                    cursect = cursects[-1]
                # no section header in the file?
                elif cursect is None:
                    raise MissingSectionHeaderError(fpname, lineno, line)
                # an option line?
                else:
                    mo = self.OPTCRE.match(line)
                    if mo:
                        optname, vi, optval = mo.group('option', 'vi', 'value')
                        if vi in ('=', ':') and '#' in optval:
                            # ';' is a comment delimiter only if it follows
                            # a spacing character
                            pos = optval.find('#')
                            if pos != -1 and optval[pos-1].isspace():
                                optval = optval[:pos]
                        optval = optval.strip()
                        # allow empty values
                        if optval == '""':
                            optval = ''
                        optname = self.optionxform(optname.rstrip())
                        cursect[optname] = optval
                    else:
                        # a non-fatal parsing error occurred.  set up the
                        # exception but keep going. the exception will be
                        # raised at the end of the file and will contain a
                        # list of all bogus lines
                        if not e:
                            e = ParsingError(fpname)
                        e.append(lineno, repr(line))
        # if any parsing errors occurred, raise an exception
        if e:
            raise e

