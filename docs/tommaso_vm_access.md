# Connecting to the DAT535 VMs (for Tommaso)

Two Ubuntu 24.04 VMs (m1.large) are running on the group's OpenStack project:

| VM | Address | Reach it via |
|---|---|---|
| `dat535-prod` | `152.94.163.166` (floating IP), private `10.53.5.207` | directly |
| `dat535-dev` | `10.53.5.34` (private only) | through prod (jump host) |

- User: `ubuntu`, port `22`, login with your own SSH key (no passwords).
- prod has a 49 GB data volume at `/data`; dev uses a plain `/data` folder.
- Both have Java 17, Python venv `~/venv` and PySpark 4.1.3.

## 1. Check your key is on the VMs

Oliwier adds the public key you sent him to `~/.ssh/authorized_keys` on both VMs. If you get `Permission denied (publickey)` in step 3, send him your `.pub` line again.

## 2. Add the VMs to your SSH config

File: `~/.ssh/config` (Windows: `C:\Users\<you>\.ssh\config`). Replace `~/.ssh/<your key>` with the private key that matches the public key you sent.

**On campus or on the UiS VPN:**

```
Host dat535-prod
    HostName 152.94.163.166
    User ubuntu
    IdentityFile ~/.ssh/<your key>

Host dat535-dev
    HostName 10.53.5.34
    User ubuntu
    IdentityFile ~/.ssh/<your key>
    ProxyJump dat535-prod
```

**Off campus without the VPN:** SSH is only open to the UiS network, so first hop through the UiS login server with your UiS account. Add this block, and add `ProxyJump uis-jump` to the `dat535-prod` block:

```
Host uis-jump
    HostName sshvm.cs.ux.uis.no
    User <your UiS username>
    IdentityFile ~/.ssh/<key registered on sshvm>
```

(On Windows, if `ProxyJump` fails, use `ProxyCommand ssh.exe -W %h:%p uis-jump` instead.)

## 3. Test

```bash
ssh dat535-prod     # answer "yes" to the fingerprint question once
exit
ssh dat535-dev      # goes through prod automatically
exit
```

VS Code: install **Remote - SSH**, then F1 → *Remote-SSH: Connect to Host* → `dat535-dev`. Use dev for experiments; prod only runs the scheduled jobs.

## About VM_HOST / VM_USER / VM_PORT

The values are `152.94.163.166`, `ubuntu` and `22`. But GitHub's own machines **cannot** reach the VMs: SSH is only open to the UiS network (152.94.0.0/16). So a workflow that SSHs from `ubuntu-latest` into the VM will time out.

The plan (build guide, Part B5 and C2) is a **self-hosted runner on each VM** (labels `dev` and `prod`). Deploy jobs then run *on* the VM with `runs-on: [self-hosted, prod]`. No SSH secrets are needed, and nothing has to be opened to the internet. If you had a different reason for these secrets, talk to Oliwier before opening port 22 wider.
