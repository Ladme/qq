## Nuitka run
uv run nuitka --onefile --output-dir=dist --output-filename=qq --clang --lto=yes --remove-output src/main.py && cp dist/qq $HOME/.local/bin