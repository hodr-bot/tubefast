{
  description = "Tubefast — YouTube Music, native and fast (Nix packaging included)";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
  };

  outputs = { self, nixpkgs }:
    let
      systems = [ "x86_64-linux" "aarch64-linux" ];
      forEachSystem = nixpkgs.lib.genAttrs systems;
    in
    {
      packages = forEachSystem (system:
        let
          pkgs = import nixpkgs { inherit system; };
          x11Libs = with pkgs.xorg; [ libX11 libXcursor libXi libXrandr libXext ];
          guiLibs = with pkgs; [ libGL wayland libxkbcommon alsa-lib ] ++ x11Libs;
        in
        {
          default = self.packages.${system}.tubefast;
          tubefast = pkgs.rustPlatform.buildRustPackage {
            pname = "tubefast";
            # Keep in sync with Cargo.toml.
            version = "0.6.0";
            src = self;
            cargoLock = { lockFile = ./Cargo.lock; };
            # Refresh with: nix build .#tubefast (paste the expected hash from the error).
            cargoHash = "sha256-AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=";
            nativeBuildInputs = with pkgs; [ pkg-config makeWrapper ];
            buildInputs = guiLibs;
            postInstall = ''
              wrapProgram $out/bin/tubefast \
                --prefix LD_LIBRARY_PATH : "${pkgs.lib.makeLibraryPath guiLibs}"
              install -Dm644 ${./linux/tubefast.desktop} \
                $out/share/applications/tubefast.desktop
              install -Dm644 ${./linux/tubefast.png} \
                $out/share/icons/hicolor/256x256/apps/tubefast.png
            '';
            meta = with pkgs.lib; {
              description = "YouTube Music, native and fast";
              homepage = "https://tubefast.dev";
              license = licenses.mit;
              platforms = [ "x86_64-linux" "aarch64-linux" ];
              mainProgram = "tubefast";
            };
          };
        });
    };
}
