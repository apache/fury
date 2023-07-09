{
  "targets": [
    {
      "target_name": "hps",
      "sources": [
        "src/fastcall.cc"
      ],
      "include_dirs"  : [
            "<!(node -e \"require('nan')\")",
            "/opt/homebrew/Cellar/v8/11.4.183.25/libexec/include"
      ],
      "cflags": ["-g"]
    }
  ]
}