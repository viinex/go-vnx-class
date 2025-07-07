#!/bin/sh

cat <<EOF > ${CURDIR}/debian/changelog
vnx-class (0.2.0-$BUILD_NUMBER) UNRELEASED; urgency=medium

  * Please refer to https://viinex.com/category/news/ for changelog

 -- Viinex Inc. <info@viinex.com>  `date -R`
EOF
