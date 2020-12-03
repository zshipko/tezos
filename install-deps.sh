pkgs=$(cat opam-list.txt | tr '\n' ' ')
opam install $pkgs
