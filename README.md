# hermes-mobile

`hermes-mobile` is a Hermes plugin that exposes the mobile API, pairing flow,
device management, uploads, and operator commands used by Talaria.

## What is here

- `plugins/hermes_mobile`: plugin package and `plugin.yaml`
- `tests/plugin`: plugin-owned test suite

## Operator surface

The plugin exposes:

- `mobile_install_or_verify`
- `mobile_generate_pairing_code`
- `hermes mobile install-or-verify --channel stable`
- `hermes mobile generate-pairing-code --target-profile <name>`

## Local test run

```bash
python3 -m unittest discover -s tests/plugin -p 'test_hermes_mobile*.py'
```

## License

MIT.
