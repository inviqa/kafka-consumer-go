# Upgrades

This document highlights breaking changes in releases that will require some migration effort in your project. As we move towards a `1.0.0` release these will be restricted to major upgrades only, but currently, whilst the API is still being fleshed out in the `0.x` releases, they may be more frequent. 

## `0.1.x` -> `0.2.0`

* `config.NewConfig()` now returns an error value, as well as a `*config.Config` value, so you will need to check for an error and handle it accordingly

