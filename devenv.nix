{ pkgs, ... }:
{
  packages = [ pkgs.gnumake ];

  # Python and the venv are managed by uv (`make install` -> `uv sync`),
  # which reads requires-python from pyproject.toml.
  languages.python = {
    enable = true;
    uv.enable = true;
  };
}
