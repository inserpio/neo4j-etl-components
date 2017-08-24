'use strict';

module.exports = function(grunt) {

  // Project configuration.
  grunt.initConfig({

    nodeunit: {
      files: ['test/**/test*.js'],
    },

    jshint: {
      options: {
        jshintrc: '.jshintrc'
      },
      gruntfile: {
        src: 'Gruntfile.js'
      },
      lib: {
        src: ['lib/**/*.js']
      },
      test: {
        src: ['test/**/*.js']
      },
    },

    watch: {
      gruntfile: {
        files: '<%= jshint.gruntfile.src %>',
        tasks: ['jshint:gruntfile']
      },
      lib: {
        files: '<%= jshint.lib.src %>',
        tasks: ['jshint:lib', 'nodeunit']
      },
      test: {
        files: '<%= jshint.test.src %>',
        tasks: ['jshint:test', 'nodeunit']
      },
    },

    copy: {
      dist: {
        files: [{
          expand: true,
          cwd: 'bin',
          src: '**/*',
          dest: 'target/ui/bin'
        }, {
          expand: true,
          cwd: 'lib',
          src: '**/*',
          dest: 'target/ui/lib'
        }, {
          expand: true,
          cwd: 'node_modules',
          src: ['**/*', '!**/grunt*/**'],
          dest: 'target/ui/node_modules'
        }, {
          expand: true,
          cwd: 'public',
          src: '**/*',
          dest: 'target/ui/public'
        }, {
          expand: true,
          cwd: 'routes',
          src: '**/*',
          dest: 'target/ui/routes'
        }, {
          expand: true,
          cwd: 'views',
          src: '**/*',
          dest: 'target/ui/views'
        }, {
          expand: true,
          cwd: '',
          src: 'app.js',
          dest: 'target/ui'
        }]
      }
    }

  });

  // These plugins provide necessary tasks.
  grunt.loadNpmTasks('grunt-contrib-nodeunit');
  grunt.loadNpmTasks('grunt-contrib-jshint');
  grunt.loadNpmTasks('grunt-contrib-watch');
  grunt.loadNpmTasks('grunt-contrib-copy');

  // Default task.
  grunt.registerTask('default', ['jshint', 'nodeunit']);

  // Build task
  grunt.registerTask('build', function (target) {
    grunt.task.run('copy:dist');
  });
};
