import gulp from 'gulp';
import babel from 'gulp-babel';
import loadPlugins from 'gulp-load-plugins';
import sourcemaps from 'gulp-sourcemaps';
import eslint from 'gulp-eslint';

const plugins = loadPlugins();

const paths = {
  lint: ['./gulpfile.babel.js', './lib/**/*.js'],
  watch: ['./gulpfile.babel.js', './lib/**', './test/**/*.js', '!test/{temp,temp/**}'],
  tests: ['./test/**/*.js', '!test/{temp,temp/**}'],
  source: ['./lib/*.js'],
  compileSource: ['./lib/**/*.js'],
  dist: ['./dist/**/*.js']
};

const plumberConf = {};

if (process.env.CI) {
  plumberConf.errorHandler = function(err) {
    throw err;
  };
}

gulp.task('compile', () => {
  return gulp
    .src(paths.compileSource)
    .pipe(sourcemaps.init())
    .pipe(babel())
    .pipe(sourcemaps.write('.'))
    .pipe(gulp.dest('./dist/'));
});

// ********* Babel Ecmascript linting
gulp.task('eslint', () => {
  return gulp
    .src(paths.lint)
    .pipe(eslint('./.eslintrc'))
    .pipe(eslint.format());
});

gulp.task('istanbul', ['compile'], cb => {
  gulp.src(paths.source)
    .pipe(plugins.istanbul()) // Covering files
    .pipe(plugins.istanbul.hookRequire()) // Force `require` to return covered files
    .on('finish', function () {
      gulp.src(paths.tests)
        .pipe(plugins.plumber(plumberConf))
        .pipe(plugins.mocha({timeout: 10000}))
        .pipe(plugins.istanbul.writeReports()) // Creating the reports after tests runned
        .on('finish', function() {
          process.chdir(__dirname);
          cb();
        });
    });
});

gulp.task('bump', ['test'], () => {
  var bumpType = plugins.util.env.type || 'patch'; // major.minor.patch

  return gulp.src(['./package.json'])
    .pipe(plugins.bump({ type: bumpType }))
    .pipe(gulp.dest('./'));
});

gulp.task('watch', ['test'], () => {
  gulp.watch(paths.watch, ['test']);
});

gulp.on('stop', function () {
  process.nextTick(function () {
    process.exit(0);
  });
});

gulp.task('test', ['eslint', 'istanbul']);

gulp.task('release', ['bump']);

gulp.task('default', ['test']);
