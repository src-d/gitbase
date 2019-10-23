pipeline {
  agent {
    kubernetes {
      label 'regression-gitbase'
      inheritFrom 'performance'
      defaultContainer 'regression-gitbase'
      nodeSelector 'srcd.host/type=jenkins-worker'
      containerTemplate {
        name 'regression-gitbase'
        image 'srcd/regression-gitbase:v0.3.4'
        ttyEnabled true
        command 'cat'
      }
    }
  }
  environment {
    GOPATH = "/go"
    GO_IMPORT_PATH = "github.com/src-d/regression-gibase"
    GO_IMPORT_FULL_PATH = "${env.GOPATH}/src/${env.GO_IMPORT_PATH}"
    GO111MODULE = "on"
    PROM_ADDRESS = "http://prom-pushgateway-prometheus-pushgateway.monitoring.svc.cluster.local:9091"
    PROM_JOB = "gitbase_perfomance"
  }
  triggers { pollSCM('0 0,12 * * *') }
  stages {
    stage('Run performance tests') {
      when { branch 'master' }
      steps {
        sh '/bin/regression --complexity=2 --csv --prom local:HEAD'
      }
    }
    stage('PR-run') {
      when { changeRequest target: 'master' }
      steps {
        sh '/bin/regression --complexity=0 remote:master local:HEAD'
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
    stage('Run bblfsh mockup tests') {
      when { branch 'master' }
      steps {
        sh '/bin/regression-bblfsh-mockups local:HEAD'
      }
    }
  }
  post {
    success {
      slackSend (color: '#2eb886', message: "SUCCESS: `${env.JOB_NAME}` <${env.BUILD_URL}|build #${env.BUILD_NUMBER}>")
    }
    failure {
      slackSend (color: '#b82e60', message: "FAILED: `${env.JOB_NAME}` <${env.BUILD_URL}|build #${env.BUILD_NUMBER}>")
    }
  }
}
