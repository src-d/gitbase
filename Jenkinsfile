pipeline {
  agent {
    kubernetes {
      label 'regression-gitbase'
      inheritFrom 'performance'
      defaultContainer 'regression-gitbase'
      nodeSelector 'srcd.host/type=jenkins-worker'
      containerTemplate {
        name 'regression-gitbase'
        image 'srcd/regression-gitbase:v0.2.0'
        ttyEnabled true
        command 'cat'
      }
    }
  }
  environment {
    GOPATH = "/go"
    GO_IMPORT_PATH = "github.com/src-d/regression-gibase"
    GO_IMPORT_FULL_PATH = "${env.GOPATH}/src/${env.GO_IMPORT_PATH}"
  }
  triggers { pollSCM('0 0,12 * * *') }
  stages {
    stage('Run') {
      when { branch 'master' }
      steps {
        sh 'apt-get install -y libonig-dev'
        sh '/bin/regression --complexity=2 --csv local:HEAD'
      }
    }
    stage('Plot') {
      when { branch 'master' }
      steps {
        script {
          plotFiles = findFiles(glob: "plot_*.csv")
          plotFiles.each {
            echo "plot ${it.getName()}"
            sh "cat ${it.getName()}"
            plot(
              group: 'performance',
              csvFileName: it.getName(),
              title: it.getName(),
              numBuilds: '100',
              style: 'line',
              csvSeries: [[
                displayTableFlag: false,
                exclusionValues: '',
                file: it.getName(),
                inclusionFlag: 'OFF',
              ]]
            )
          }
        }
      }
    }
  }
}
