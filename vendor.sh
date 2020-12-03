GIT_ROOT=..
VENDORS=vendors/dev

## mirage/repr

mkdir -p ${VENDORS}/repr
cp ${GIT_ROOT}/repr/repr.opam ${VENDORS}/repr
cp -R ${GIT_ROOT}/repr/src/repr ${VENDORS}/repr
echo '(lang dune 2.7)' > ${VENDORS}/repr/dune-project

mkdir -p ${VENDORS}/ppx_repr
cp ${GIT_ROOT}/repr/ppx_repr.opam ${VENDORS}/ppx_repr
cp -R ${GIT_ROOT}/repr/src/ppx_repr ${VENDORS}/ppx_repr
echo '(lang dune 2.7)' > ${VENDORS}/ppx_repr/dune-project

## index

mkdir -p ${VENDORS}/index
cp ${GIT_ROOT}/index/index.opam ${VENDORS}/index
cp -R ${GIT_ROOT}/index/src/ ${VENDORS}/index
echo '(lang dune 2.7)' > ${VENDORS}/index/dune-project

## irmin

mkdir -p ${VENDORS}/irmin
cp ${GIT_ROOT}/irmin/irmin.opam ${VENDORS}/irmin
cp -R ${GIT_ROOT}/irmin/src/irmin ${VENDORS}/irmin
echo '(lang dune 2.7)' > ${VENDORS}/irmin/dune-project

mkdir -p ${VENDORS}/irmin-pack
cp ${GIT_ROOT}/irmin/irmin-pack.opam ${VENDORS}/irmin-pack
cp -R ${GIT_ROOT}/irmin/src/irmin-pack ${VENDORS}/irmin-pack
echo '(lang dune 2.7)' > ${VENDORS}/irmin-pack/dune-project

mkdir -p ${VENDORS}/irmin-layers
cp ${GIT_ROOT}/irmin/irmin-layers.opam ${VENDORS}/irmin-layers
cp -R ${GIT_ROOT}/irmin/src/irmin-layers ${VENDORS}/irmin-layers
echo '(lang dune 2.7)' > ${VENDORS}/irmin-layers/dune-project

mkdir -p ${VENDORS}/ppx_irmin
cp ${GIT_ROOT}/irmin/ppx_irmin.opam ${VENDORS}/ppx_irmin
cp -R ${GIT_ROOT}/irmin/src/ppx_irmin ${VENDORS}/ppx_irmin
echo '(lang dune 2.7)' > ${VENDORS}/ppx_irmin/dune-project

## remove dev pins

find ${VENDORS} -name "*.opam" | xargs sed -i "" '/\.dev/d'
find ${VENDORS} -name "*.opam" | xargs sed -i "" '/pin-depends:/,+2 d'
find ${VENDORS} -name "*.opam" | xargs sed -i "" 's/{= version}//'
find ${VENDORS} -name "*.opam" | xargs sed -i "" '/with-test/d'
