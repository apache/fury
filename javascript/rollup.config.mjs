import typescript from '@rollup/plugin-typescript';

export default [
  {
    input: './index.ts', 
    output: {
      preserveModules: true,
      dir: './dist/', 
      format: 'cjs', 
    },
    external: [/(.)*.node$/],
    plugins: [
      typescript({
        compilerOptions: {
          declaration: true,
          declarationDir: "./dist/types"
        }
      }),
    ]
  },
  {
    input: './index.ts', 
    output: {
      preserveModules: true,
      dir: './dist/esm/', 
      entryFileNames: '[name].mjs',
      format: 'es', 
    },
    external: [/(.)*.node$/],
    plugins: [
      typescript({
        compilerOptions: {
          outDir: "./dist/esm"
        }
      }), 
    ]
  }
];