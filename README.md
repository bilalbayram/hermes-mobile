# hermes-mobile

`hermes-mobile` is a Hermes plugin that exposes the mobile API, pairing flow,
device management, uploads, and operator commands used by Talaria.

## What is here

- `plugin.yaml`: Hermes plugin manifest at the repo root
- `hermes_mobile`: plugin package implementation
- `tests/plugin`: plugin-owned test suite

## Operator surface

The plugin exposes:

- `mobile_install_or_verify`
- `mobile_generate_pairing_code`
- `mobile_prepare_connection_bundle`
- `hermes mobile install-or-verify --channel stable`
- `hermes mobile generate-pairing-code --target-profile <name>`
- `hermes mobile prepare-connection-bundle --base-url <https-url> --target-profile <name>`
- `hermes talaria-mobile prepare-connection-bundle --base-url <https-url> --target-profile <name>`

## Talaria connection bundles

For Talaria onboarding, the Hermes agent should:

- verify the stable `github.com/bilalbayram/hermes-mobile` plugin first
- prefer an existing private HTTPS path before suggesting public exposure
- suggest Tailscale first when the machine already uses it
- ask the human before creating a new public endpoint
- return a single `TALARIA-CONNECT` bundle with the HTTPS URL, profile, and one-time pairing code

## Local test run

```bash
python3 -m unittest discover -s tests/plugin -p 'test_hermes_mobile*.py'
```

## License

MIT.
