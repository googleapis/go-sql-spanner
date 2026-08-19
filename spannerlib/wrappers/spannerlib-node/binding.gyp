{
  'variables': {
    'enable_thin_lto%': 'false',
    'enable_lto%': 'false',
    'use_lld%': 'false',
    'lto_jobs%': ''
  },
  'target_defaults': {
    'conditions': [
      ['OS=="win"', {
        'msvs_settings': {
          'VCCLCompilerTool': {
            'ExceptionHandling': 1,
            'EnablePREfast': 'false',
            'LanguageStandard': 'stdcpp20',
            'AdditionalOptions!': [
              '-flto=thin',
              '/flto=thin',
              '-flto',
              '/flto'
            ]
          },
          'VCLinkerTool': {
            'LinkTimeCodeGeneration': 0,
            'AdditionalOptions!': [
              '/opt:lldltojobs=1',
              '/opt:lldltojobs=2',
              '/opt:lldltojobs=4',
              '/opt:lldltojobs=8',
              '-flto=thin',
              '/flto=thin',
              '-flto',
              '/flto'
            ]
          }
        }
      }]
    ]
  },
  'targets': [
    {
      'target_name': 'spanner_napi',
      'sources': [ 'src/cpp/addon.cc' ],
      'include_dirs': [
        '<!@(node -p "require(\'node-addon-api\').include")',
        '../../shared'
      ],
      'dependencies': [
        '<!(node -p "require(\'node-addon-api\').gyp")'
      ],
      'defines': [
        'NAPI_CPP_EXCEPTIONS'
      ],
      'cflags!': [ '-fno-exceptions' ],
      'cflags_cc!': [ '-fno-exceptions' ],
      'xcode_settings': {
        'MACOSX_DEPLOYMENT_TARGET': '10.15',
        'GCC_ENABLE_CPP_EXCEPTIONS': 'YES',
        'CLANG_CXX_LIBRARY': 'libc++',
        'CLANG_CXX_LANGUAGE_STANDARD': 'c++20'
      },
      'conditions': [
        ['OS=="mac"', {
          'libraries': [
            '<(module_root_dir)/../../shared/libspanner.a'
          ]
        }],
        ['OS=="linux"', {
          'cflags_cc': [
            '-std=c++20'
          ],
          'libraries': [
            '<(module_root_dir)/../../shared/libspanner.a'
          ]
        }],
        ['OS=="win"', {
          'libraries': [
            '<(module_root_dir)/../../shared/libspanner.lib'
          ],
          'copies': [
            {
              'destination': '<(PRODUCT_DIR)',
              'files': [
                '<(module_root_dir)/../../shared/libspanner.dll'
              ]
            }
          ]
        }]
      ]
    }
  ]
}
