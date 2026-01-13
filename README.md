# canfar-vospace

Want to use vostool like commands on new CANFAR nodes, but CANFAR CLI doesn't yet include VOSpace cammands. This application is a proof-of-concept for achiving that

# Dependanies

Need to have both vostool and CANFAR CLI installed and you may need to export VOSPACE_WEBSERVICE to point at your canfar registry

Also you will need typer

# Installation

Just need the cvos.py file either clone the repo

```console
git clone https://github.com/DrWhatson/canfar-vospace.git
```

or just download the file

# Running

```console
python3 cvos.py --help
vos$ python3 cvos.py --help
@SRCnet-Sweden

 Usage: cvos.py [OPTIONS] COMMAND [ARGS]...

╭─ Options ───────────────────────────────────────────────────────────╮
│ --install-completion          Install completion for the current    │
│                               shell.                                │
│ --show-completion             Show completion for the current       │
│                               shell, to copy it or customize the    │
│                               installation.                         │
│ --help                        Show this message and exit.           │
╰─────────────────────────────────────────────────────────────────────╯
╭─ Commands ──────────────────────────────────────────────────────────╮
│ ls      Lists information about a VOSpace DataNode or the contents  │
│         of a ContainerNode.                                         │
│ cp      Copy files to and from VOSpace. Always recursive.           │
│ rm      Remove a vospace data node; fails if container node or node │
│         is locked.                                                  │
│ mkdir   Create a new VOSpace ContainerNode (directory).             │
│ mv      Move node to newNode, if newNode is a container then move   │
│         node into newNode.                                          │
╰─────────────────────────────────────────────────────────────────────╯
```
