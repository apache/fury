## Development
### Python setup
```bash
cd python
python setup.py develop
```
* cython: use `cython --cplus -a  pyfury/_serialization.pyx` to produce an annotated
  html file of the source code. Then we can analyze interaction between
  Python objects and Python’s C-API.

* Debug
  https://cython.readthedocs.io/en/latest/src/userguide/debugging.html
```bash
FURY_DEBUG=true python setup.py build_ext --inplace
# For linux
cygdb build
```

### Debug Crash
Enable core dump on Macos Monterey 12.1
```bash
 /usr/libexec/PlistBuddy -c "Add :com.apple.security.get-task-allow bool true" tmp.entitlements     
codesign -s - -f --entitlements tmp.entitlements /Users/chaokunyang/anaconda3/envs/py3.8/bin/python
ulimit -c unlimited

python fury_serializer.py
ls -al /cores
```

## Profiling
### C++
```bash
# Dtrace
sudo dtrace -x ustackframes=100 -n 'profile-99 /pid == 73485 && arg1/ { @[ustack()] = count(); } tick-60s { exit(0); }' -o out.stack
sudo stackcollapse.pl out.stack > out.folded
sudo flamegraph.pl out.folded > out.svg
```

## CI
### Login into ci machine
```yaml
      - name: Setup tmate session
        uses: mxschmitt/action-tmate@v3
```

## Website
Fury website are static pages hosted by github pages under https://github.com/fury-project/fury-sites.

All updates about docs under [guide](./docs/guide) and [benchmarks](./docs/benchmarks) will be synced to [fury-sites](https://github.com/fury-project/fury-sites) automatically.

If you want write a blog, or update other contents about fury website, please submit PR to [fury-sites](https://github.com/fury-project/fury-sites).