import typer
from canfar.context import Context
import vos
from vos.vos import SortNodeProperty, CADC_GMS_PREFIX, convert_vospace_time_to_seconds
from vos import md5_cache
import sys
import time
import math
import logging
import errno
import os
import re
import glob
import warnings
from cadcutils import exceptions
from urllib.parse import urlparse

context = Context()
active_context = context.config.active
ctx = context.config.contexts[active_context]
token = ctx.token.access
client=vos.Client(vospace_token=token)

app = typer.Typer()

# Global flag for human-readable sizes
human_readable = False

def size_format(size):
    """Format a size value for listing"""
    try:
        size = float(size)
    except Exception as ex:
        logging.debug(str(ex))
        size = 0.0
    if human_readable:
        size_unit = ['B', 'K', 'M', 'G', 'T']
        try:
            length = float(size)
            scale = int(math.log(length) / math.log(1024))
            length = "%.0f%s" % (length / (1024.0 ** scale), size_unit[scale])
        except Exception:
            length = str(int(size))
    else:
        length = str(int(size))
    return "%12s " % length


def date_format(epoch):
    """given an epoch, return a unix-ls like formatted string"""
    time_tuple = time.localtime(epoch)
    if time.localtime().tm_year != time_tuple.tm_year:
        return time.strftime('%b %d  %Y ', time_tuple)
    return time.strftime('%b %d %H:%M ', time_tuple)


__LIST_FORMATS__ = {'permissions': lambda value: "{:<11}".format(value),
                    'creator': lambda value: " {:<20}".format(value),
                    'readGroup': lambda value: " {:<15}".format(
                        value.replace(CADC_GMS_PREFIX, "")),
                    'writeGroup': lambda value: " {:<15}".format(
                        value.replace(CADC_GMS_PREFIX, "")),
                    'isLocked': lambda value: " {:<8}".format(["", "LOCKED"][
                        value == "true"]),
                    'size': size_format,
                    'date': date_format}


def _get_sort_key(node, sort):
    if sort == SortNodeProperty.LENGTH:
        return int(node.props['length'])
    elif sort == SortNodeProperty.DATE:
        return convert_vospace_time_to_seconds(node.props['date'])
    else:
        return node.name


def _display_target(columns, row):
    name_string = row.name
    info = row.get_info()
    for col in columns:
        value = info.get(col, None)
        value = value is not None and value or ""
        if col in __LIST_FORMATS__:
            sys.stdout.write(__LIST_FORMATS__[col](value))
        if info["permissions"][0] == 'l':
            name_string = "%s -> %s" % (
                row.name, info['target'])
    sys.stdout.write("%s\n" % name_string)


@app.command()
def ls(
    uri: str,
    long: bool = typer.Option(False, "--long", "-l", help="verbose listing sorted by name"),
    group: bool = typer.Option(False, "--group", "-g", help="display group read/write information"),
    human: bool = typer.Option(False, "--human", "-h", help="make sizes human readable"),
    size_sort: bool = typer.Option(False, "--Size", "-S", help="sort files by size"),
    reverse: bool = typer.Option(False, "--reverse", "-r", help="reverse the sort order"),
    time_sort: bool = typer.Option(False, "--time", "-t", help="sort by time copied to VOSpace")
):
    """Lists information about a VOSpace DataNode or the contents of a ContainerNode."""
    global human_readable
    human_readable = human

    # set which columns will be printed
    columns = []
    if long or group:
        columns = ['permissions']
        if long:
            columns.extend(['creator'])
        columns.extend(['readGroup', 'writeGroup', 'isLocked', 'size', 'date'])

    files = []
    dirs = []

    # determine if their is a sorting order
    if size_sort:
        sort = SortNodeProperty.LENGTH
    elif time_sort:
        sort = SortNodeProperty.DATE
    else:
        sort = None

    if sort is None and reverse is False:
        order = None
    elif reverse:
        order = 'asc' if sort else 'desc'
    else:
        order = 'desc' if sort else 'asc'

    if not client.is_remote_file(file_name=uri):
        typer.echo(f"Invalid node name: {uri}", err=True)
        raise typer.Exit(1)

    logging.debug("getting listing of: %s" % str(uri))

    targets = client.glob(uri)

    # segregate files from directories
    for target in targets:
        target_node = client.get_node(target)
        if not long or target.endswith('/'):
            while target_node.islink():
                target_node = client.get_node(target_node.target)
        if target_node.isdir():
            dirs.append((_get_sort_key(target_node, sort),
                        target_node, target))
        else:
            files.append((_get_sort_key(target_node, sort),
                         target_node))

    for f in sorted(files, key=lambda ff: ff[0],
                    reverse=(order == 'desc')):
        _display_target(columns, f[1])

    for d in sorted(dirs, key=lambda dd: dd[0], reverse=(order == 'desc')):
        n = d[1]
        if (len(dirs) + len(files)) > 1:
            sys.stdout.write('\n{}:\n'.format(n.name))
            if long:
                sys.stdout.write('total: {}\n'.format(
                    int(n.get_info()['size'])))
        for row in client.get_children_info(d[2], sort, order):
            _display_target(columns, row)
        
@app.command()
def cp(
    source: list[str] = typer.Argument(..., help="file/directory/dataNode/containerNode to copy from"),
    destination: str = typer.Argument(..., help="file/directory/dataNode/containerNode to copy to"),
    exclude: str = typer.Option(None, "--exclude", help="skip files that match pattern (overrides include)"),
    include: str = typer.Option(None, "--include", help="only copy files that match pattern"),
    interrogate: bool = typer.Option(False, "-i", "--interrogate", help="Ask before overwriting files"),
    follow_links: bool = typer.Option(False, "-L", "--follow-links", help="follow symbolic links. Default is to not follow links."),
    ignore: bool = typer.Option(False, "--ignore", help="ignore errors and continue with recursive copy"),
    head: bool = typer.Option(False, "--head", help="copy only the headers of a file from vospace")
):
    """Copy files to and from VOSpace. Always recursive."""

    class Nonlocal():
        # workaround for nonlocal scope
        exit_code = 0

    dest = destination
    this_destination = dest

    if not client.is_remote_file(dest):
        dest = os.path.abspath(dest)

    cutout_pattern = re.compile(
        r'(.*?)(?P<cutout>(\[[\-+]?[\d*]+(:[\-+]?[\d*]+)?'
        r'(,[\-+]?[\d*]+(:[\-+]?[\d*]+)?)?\])+)$')

    ra_dec_cutout_pattern = re.compile(r"([^()]*?)"
                                       r"(?P<cutout>\("
                                       r"(?P<ra>[\-+]?\d*(\.\d*)?),"
                                       r"(?P<dec>[\-+]?\d*(\.\d*)?),"
                                       r"(?P<rad>\d*(\.\d*)?)\))?")

    def get_node(filename, limit=None):
        """Get node, from cache if possible"""
        return client.get_node(filename, limit=limit)

    def isdir(filename):
        logging.debug("Doing an isdir on %s" % filename)
        if client.is_remote_file(filename):
            return client.isdir(filename)
        else:
            return os.path.isdir(filename)

    def islink(filename):
        logging.debug("Doing an islink on %s" % filename)
        if client.is_remote_file(filename):
            try:
                return get_node(filename).islink()
            except exceptions.NotFoundException:
                return False
        else:
            return os.path.islink(filename)

    def access(filename, mode):
        """Check if the file can be accessed."""
        logging.debug("checking for access %s " % filename)
        if client.is_remote_file(filename):
            try:
                node = get_node(filename, limit=0)
                return node is not None
            except (exceptions.NotFoundException, exceptions.ForbiddenException,
                    exceptions.UnauthorizedException):
                return False
        else:
            return os.access(filename, mode)

    def listdir(dirname):
        """Walk through the directory structure a al os.walk"""
        logging.debug("getting a dirlist %s " % dirname)
        if client.is_remote_file(dirname):
            return client.listdir(dirname, force=True)
        else:
            return os.listdir(dirname)

    def mkdir(filename):
        logging.debug("Making directory %s " % filename)
        if client.is_remote_file(filename):
            return client.mkdir(filename)
        else:
            return os.mkdir(filename)

    def get_md5(filename):
        logging.debug("getting the MD5 for %s" % filename)
        if client.is_remote_file(filename):
            return get_node(filename).props.get('MD5', vos.ZERO_MD5)
        else:
            return md5_cache.MD5Cache.compute_md5(filename)

    def lglob(pathname):
        if client.is_remote_file(pathname):
            return client.glob(pathname)
        else:
            return glob.glob(pathname)

    def copy_file(source_name, destination_name, exclude_arg=None, include_arg=None,
             interrogate_arg=False, overwrite=False, ignore_arg=False, head_arg=False):
        """
        Send source_name to destination, possibly looping over contents if
        source_name points to a directory.
        """
        try:
            if not follow_links and islink(source_name):
                logging.info("{}: Skipping (symbolic link)".format(source_name))
                return
            if isdir(source_name):
                # make sure the destination exists...
                if not isdir(destination_name):
                    mkdir(destination_name)
                # for all files in the current source directory copy them to
                # the destination directory
                for filename in listdir(source_name):
                    logging.debug("%s -> %s" % (filename, source_name))
                    copy_file(os.path.join(source_name, filename),
                         os.path.join(destination_name, filename),
                         exclude_arg, include_arg, interrogate_arg, overwrite, ignore_arg,
                         head_arg)
            else:
                if interrogate_arg:
                    if access(destination_name, os.F_OK):
                        sys.stderr.write(
                            "File %s exists.  Overwrite? (y/n): " %
                            destination_name)
                        ans = sys.stdin.readline().strip()
                        if ans != 'y':
                            raise Exception("File exists")

                skip = False
                if exclude_arg is not None:
                    for thisIgnore in exclude_arg.split(','):
                        if not destination_name.find(thisIgnore) < 0:
                            skip = True
                            continue

                if include_arg is not None:
                    skip = True
                    for thisIgnore in include_arg.split(','):
                        if not destination_name.find(thisIgnore) < 0:
                            skip = False
                            continue

                if not skip:
                    logging.info("%s -> %s " % (source_name, destination_name))
                niters = 0
                while not skip:
                    try:
                        logging.debug("Starting call to copy")
                        client.copy(source_name, destination_name, head=head_arg)
                        logging.debug("Call to copy returned")
                        break
                    except Exception as client_exception:
                        logging.debug("{}".format(client_exception))
                        if getattr(client_exception, 'errno', -1) == 104:
                            # 104 is connection reset by peer.
                            # Try again on this error
                            logging.warning(str(client_exception))
                            Nonlocal.exit_code += \
                                getattr(client_exception, 'errno', -1)
                        elif getattr(client_exception, 'errno', -1) == errno.EIO:
                            # retry on IO errors
                            logging.warning(
                                "{0}: Retrying".format(client_exception))
                            pass
                        elif ignore_arg:
                            if niters > 100:
                                logging.error(
                                    "%s (skipping after %d attempts)" % (
                                        str(client_exception), niters))
                                skip = True
                            else:
                                logging.error(
                                    "%s (retrying)" % str(client_exception))
                                time.sleep(5)
                                niters += 1
                        else:
                            raise client_exception

        except OSError as os_exception:
            logging.debug(str(os_exception))
            if getattr(os_exception, 'errno', -1) == errno.EINVAL:
                # not a valid uri, just skip those...
                logging.warning("%s: Skipping" % str(os_exception))
                Nonlocal.exit_code += getattr(os_exception, 'errno', -1)
            else:
                typer.echo(f"Error: {os_exception}", err=True)
                raise typer.Exit(1)

    # main loop
    source_arg = source[0]
    try:
        for source_pattern in source:

            if head and not client.is_remote_file(source_pattern):
                logging.error("head only works for source files in vospace")
                continue

            # define this empty cutout string.  Then we strip possible cutout
            # strings off the end of the pattern before matching.  This allows
            # cutouts on the vos service. The shell does pattern matching for
            # local files, so don't run glob on local files.
            if not client.is_remote_file(source_pattern):
                sources = [source_pattern]
            else:
                cutout_match = cutout_pattern.search(source_pattern)
                cutout = None
                if cutout_match is not None:
                    source_pattern = cutout_match.group(1)
                    cutout = cutout_match.group('cutout')
                else:
                    ra_dec_match = ra_dec_cutout_pattern.search(source_pattern)
                    if ra_dec_match is not None:
                        cutout = ra_dec_match.group('cutout')
                logging.debug("cutout: {}".format(cutout))
                sources = lglob(source_pattern)
                if cutout is not None:
                    # stick back on the cutout pattern if there was one.
                    sources = [s + cutout for s in sources]
            for source_arg in sources:
                if not client.is_remote_file(source_arg):
                    source_arg = os.path.abspath(source_arg)
                # the source must exist, of course...
                if not access(source_arg, os.R_OK):
                    raise Exception("Can't access source: %s " % source_arg)

                if not follow_links and islink(source_arg):
                    logging.info("{}: Skipping (symbolic link)".format(source_arg))
                    continue

                # copying inside VOSpace not yet implemented
                if client.is_remote_file(source_arg) and \
                   client.is_remote_file(dest):
                    raise Exception(
                        "Can not (yet) copy from VOSpace to VOSpace.")

                this_destination = dest
                if isdir(source_arg):
                    if not follow_links and islink(source_arg):
                        continue
                    logging.debug("%s is a directory or link to one" % source_arg)
                    # To mimic unix fs behaviours if copying a directory and
                    # the destination directory exists then the actual
                    # destination in a recursive copy is the destination +
                    # source basename.
                    # This has an odd behaviour if more than one directory is
                    # given as a source and the copy is recursive.
                    if access(dest, os.F_OK):
                        if not isdir(dest):
                            raise Exception(
                                "Can't write a directory (%s) to a file (%s)" %
                                (source_arg, dest))
                        # directory exists so we append the end of source to
                        # that (UNIX behaviour)
                        this_destination = os.path.normpath(
                            os.path.join(dest, os.path.basename(source_arg)))
                    elif len(source) > 1:
                        raise Exception(
                            ("vcp can not copy multiple things into a"
                             "non-existent location (%s)") % dest)
                elif dest[-1] == '/' or isdir(dest):
                    # we're copying into a directory
                    this_destination = os.path.join(dest,
                                                    os.path.basename(source_arg))
                copy_file(source_arg, this_destination, exclude_arg=exclude,
                     include_arg=include,
                     interrogate_arg=interrogate, overwrite=False,
                     ignore_arg=ignore, head_arg=head)

    except KeyboardInterrupt as ke:
        logging.info("Received keyboard interrupt. Execution aborted...\n")
        Nonlocal.exit_code = getattr(ke, 'errno', -1)
    except Exception as e:
        if re.search('NodeLocked', str(e)) is not None:
            msg = "Use vlock to unlock the node before copying to {}.".format(
                this_destination)
            typer.echo(f"Error: {e}\n{msg}", err=True)
        elif getattr(e, 'errno', -1) == errno.EREMOTE:
            msg = "Failure at remote server while copying {0} -> {1}\n".format(
                    source_arg, dest)
            typer.echo(f"Error: {e}\n{msg}", err=True)
        else:
            typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1)

    if Nonlocal.exit_code:
        raise typer.Exit(Nonlocal.exit_code)

@app.command()
def rm(
    node: list[str] = typer.Argument(..., help="file, link or possibly directory to delete from VOSpace"),
    recursive: bool = typer.Option(False, "-R", "--recursive", help="Delete a file or directory even if it's not empty.")
):
    """Remove a vospace data node; fails if container node or node is locked."""

    try:
        for node_path in node:
            if not client.is_remote_file(node_path):
                raise Exception(
                    '{} is not a valid VOSpace handle'.format(node_path))
            if recursive:
                successes, failures = client.recursive_delete(node_path)
                if failures:
                    logging.error('WARN. deleted count: {}, failed count: '
                                  '{}\n'.format(successes, failures))
                    raise typer.Exit(-1)
                else:
                    logging.info(
                        'DONE. deleted count: {}\n'.format(successes))
            else:
                if not node_path.endswith('/'):
                    if client.get_node(node_path).islink():
                        logging.info('deleting link {}'.format(node_path))
                        client.delete(node_path)
                    elif client.isfile(node_path):
                        logging.info('deleting {}'.format(node_path))
                        client.delete(node_path)
                elif client.isdir(node_path):
                    raise Exception('{} is a directory'.format(node_path))
                else:
                    raise Exception('{} is not a directory'.format(node_path))

    except Exception as ex:
        typer.echo(f"Error: {ex}", err=True)
        raise typer.Exit(1)

@app.command()
def mkdir(
    container_node: str = typer.Argument(..., help="Name of the container node to create"),
    parents: bool = typer.Option(False, "-p", help="Create intermediate directories as required")
):
    """Create a new VOSpace ContainerNode (directory)."""

    logging.info(
        "Creating ContainerNode (directory) {}".format(container_node))

    try:
        this_dir = container_node

        dir_names = []
        if parents:
            while not client.access(this_dir):
                dir_names.append(os.path.basename(this_dir))
                this_dir = os.path.dirname(this_dir)
            while len(dir_names) > 0:
                this_dir = os.path.join(this_dir, dir_names.pop())
                client.mkdir(this_dir)
        else:
            client.mkdir(this_dir)

    except Exception as ex:
        typer.echo(f"Error: {ex}", err=True)
        raise typer.Exit(1)

@app.command()
def mv(
    source: str = typer.Argument(..., help="The name of the node to move"),
    destination: str = typer.Argument(..., help="VOSpace destination to move source to")
):
    """Move node to newNode, if newNode is a container then move node into newNode."""

    try:
        if not client.is_remote_file(source):
            raise ValueError('Source {} is not a remote node'.format(source))
        if not client.is_remote_file(destination):
            raise ValueError(
                'Destination {} is not a remote node'.format(destination))
        if urlparse(source).scheme != urlparse(destination).scheme:
            raise ValueError('Move between services not supported')
        logging.info("{} -> {}".format(source, destination))
        client.move(source, destination)
    except Exception as ex:
        typer.echo(f"Error: {ex}", err=True)
        raise typer.Exit(1)


if __name__ == "__main__":
    app()