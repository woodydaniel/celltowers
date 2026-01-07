# Hetzner CellMapper Scraper Host

Use these settings so any agent (running on **this same workstation**) can SSH into the server without guessing.

| Item | Value |
|------|-------|
| Host / IP | `91.99.49.172` |
| SSH User | `root` |
| SSH Port | `22` (default) |
| SSH Key (private) | `~/.ssh/hetzner_cellmapper.pem` |

## Quick-connect
```bash
ssh -i ~/.ssh/hetzner_cellmapper.pem root@91.99.49.172
```

## If the key isn’t already loaded in your ssh-agent
```bash
eval "$(ssh-agent -s)"    # start agent if needed
ssh-add ~/.ssh/hetzner_cellmapper.pem
```

After that, plain `ssh root@91.99.49.172` works.

### Notes
* The public key corresponding to `hetzner_cellmapper.pem` is in `/root/.ssh/authorized_keys` on the server.
* No additional firewall rules—port 22 open to the world.
* Project lives at `/root/cellmapper`; activate venv with `source venv/bin/activate`.

