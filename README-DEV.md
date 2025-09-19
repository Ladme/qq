## Nuitka run
uv run nuitka --onefile --output-dir=dist --output-filename=qq --clang --lto=yes --deployment --remove-output src/main.py && cp dist/qq $HOME/.local/bin