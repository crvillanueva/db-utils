set shell := ["zsh", "-cu"]
set dotenv-load

python_bin:="/usr/bin/python3.11"

install:
    pipx install --force --editable . --python {{python_bin}}
test:
    pytest -rP
cp-test-db:
    cp "$CODE_DIR/esc-gui-control-op/control_operacional.db" ./test_db.db
