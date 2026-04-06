{
  "targets": [
    {
      "target_name": "spanner_napi",
      "sources": [ "src/cpp/addon.cc" ],
      "include_dirs": [
        "<!@(node -p \"require('node-addon-api').include\")",
        "../../shared"
      ],
      "dependencies": [
        "<!(node -p \"require('node-addon-api').gyp\")"
      ],
      "cflags!": [ "-fno-exceptions" ],
      "cflags_cc!": [ "-fno-exceptions" ],
      "xcode_settings": {
        "GCC_ENABLE_CPP_EXCEPTIONS": "YES",
        "CLANG_CXX_LIBRARY": "libc++",
        "MACOSX_DEPLOYMENT_TARGET": "10.15"
      },
      "msvs_settings": {
        "VCCLCompilerTool": { "ExceptionHandling": 1 }
      },
      "conditions": [
        ["OS=='mac'", {
          "libraries": [
            "<(module_root_dir)/../../../shared/libspanner.so"
          ],
          "xcode_settings": {
            "OTHER_LDFLAGS": [
               "-Wl,-rpath,<(module_root_dir)/../../../shared"
            ]
          }
        }],
        ["OS=='linux'", {
          "libraries": [
            "<(module_root_dir)/../../../shared/libspanner.so"
          ],
          "ldflags": [
            "-Wl,-rpath,'$$ORIGIN/../../../shared'"
          ]
        }]
      ]
    }
  ]
}
