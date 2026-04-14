# hermes-mobile

`hermes-mobile` is a Hermes plugin that exposes the mobile API, pairing flow,
device management, uploads, and operator commands used by Talaria.

## What is here

- `plugin.yaml`: Hermes plugin manifest at the repo root
- `plugins/hermes_mobile`: plugin package implementation
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
