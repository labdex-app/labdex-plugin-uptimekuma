# Uptime Kuma

A [labdex](https://github.com/reykimedev/labdex) plugin. Keeps an Uptime Kuma monitor for every service with a link (plan, then apply) and reads their status back, per service and per host. Needs python-socketio (in the Docker image).

**Version** 1.0 · **Works with** labdex >=1.0,<2 · **Plugin id** `uptimekuma`

> [!WARNING]
> Built with AI, for home labs only: labdex and its plugins were written largely with an AI
> coding assistant (Claude), directed and reviewed by a human. They haven't had an independent
> security audit and may contain security problems. Run them on a trusted home network.

## Install

It ships with labdex: **Plugins › Marketplace**, then *Install* (nothing is downloaded). Updates arrive through the official Marketplace (signed with the labdex project key).

## Settings

| Setting | Notes |
| --- | --- |
| URL |  |
| User name |  |
| Password | An account without two-factor login (Uptime Kuma's API needs a password login) |
| Check the TLS certificate |  |
| Monitor | links: services with an http(s) link; tcp: also every other TCP service (port check) |
| Check every … seconds |  |
| Retries before down |  |
| Notification ids | Uptime Kuma notification ids to attach to new monitors |
| Accept any certificate (https monitors) |  |

## Actions

- **Check connection** (run)
- **Sync monitors** (plan): Creates, changes and removes the monitors labdex manages (marked in their description)
- **Read status** (run)

## Releasing

Bump `version` in `plugin.yaml`, add a `## <version>` section to `CHANGELOG.md`, commit, then
`git tag v<version> && git push --tags`. The release workflow signs the plugin with the
`LABDEX_SIGNING_KEY` secret and attaches `uptimekuma-<version>.zip` to a GitHub release.
