{
  "targets": [
    {
      "target_name": "hps",
      "sources": [
        "src/fastcall.cc",
        "src/v8-fast-api-calls.h"
      ],
      "include_dirs"  : [
            "<!(node -e \"require('nan')\")"
      ],
      "cflags": ["-g"]
    }
  ]
}