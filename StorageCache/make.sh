cd nginx-release-1.15.1
./auto/configure --with-http_slice_module
make -j4
cp objs/nginx ..
