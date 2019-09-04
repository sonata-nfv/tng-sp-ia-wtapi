pipeline {
  agent any
  stages {
    stage('Container Build') {
      parallel {
        stage('Container Build') {
          steps {
            echo 'Building...'
          }
        }
        stage('Building tng-sp-ia-wtapi') {
          steps {
            sh 'docker build -t registry.sonata-nfv.eu:5000/tng-sp-ia-wtapi .'
          }
        }
      }
    }
    stage('Unit Test') {
      parallel {
        stage('Unit Tests') {
          steps {
            echo 'Performing Unit Tests'
          }
        }
        stage('Running Unit Tests') {
          steps {
            echo 'TODO'
          }
        }
      }
    }
    stage('Containers Publication') {
      parallel {
        stage('Containers Publication') {
          steps {
            echo 'Publication of containers in local registry....'
          }
        }
        stage('Publishing tng-sp-ia-wtapi') {
          steps {
            sh 'docker push registry.sonata-nfv.eu:5000/tng-sp-ia-wtapi'
          }
        }
      }
    }
    stage('Deployment in Integration') {
      parallel {
        stage('Deployment in Integration') {
          steps {
            echo 'Deploying in integration...'
          }
        }
        stage('Deploying') {
          steps {
            sh 'rm -rf tng-devops || true'
            sh 'git clone https://github.com/sonata-nfv/tng-devops.git'
            dir(path: 'tng-devops') {
              sh 'ansible-playbook roles/sp.yml -i environments -e "target=pre-int-sp component=infrastructure-abstraction"'
            }
          }
        }
      }
    }
    stage('Promoting containers to integration env') {
      when {
         branch 'master'
      }
      parallel {
        stage('Publishing containers to int') {
          steps {
            echo 'Promoting containers to integration'
          }
        }
        stage('tng-sp-ia-wtapi') {
          steps {
            sh 'docker tag registry.sonata-nfv.eu:5000/tng-sp-ia-wtapi:latest registry.sonata-nfv.eu:5000/tng-sp-ia-wtapi:int'
            sh 'docker push registry.sonata-nfv.eu:5000/tng-sp-ia-wtapi:int'
          }
        }
        stage('Promoting to integration') {
          when {
            branch 'master'
          }
          steps {
            sh 'docker tag registry.sonata-nfv.eu:5000/tng-sp-ia-wtapi:latest registry.sonata-nfv.eu:5000/tng-sp-ia-wtapi:int'
            sh 'docker push registry.sonata-nfv.eu:5000/tng-sp-ia-wtapi:int'
            sh 'rm -rf tng-devops || true'
            sh 'git clone https://github.com/sonata-nfv/tng-devops.git'
            dir(path: 'tng-devops') {
              sh 'ansible-playbook roles/sp.yml -i environments -e "target=int-sp component=infrastructure-abstraction"'
            }
          }
        }
      }
    }
    stage('Promoting release v5.0') {
      when {
        branch 'v5.0'
      }
      stages {
        stage('Generating release') {
          steps {
            sh 'docker tag registry.sonata-nfv.eu:5000/tng-sp-ia-wtapi:latest registry.sonata-nfv.eu:5000/tng-sp-ia-wtapi:v5.0'
            sh 'docker tag registry.sonata-nfv.eu:5000/tng-sp-ia-wtapi:latest sonatanfv/tng-sp-ia-wtapi:v5.0'
            sh 'docker push registry.sonata-nfv.eu:5000/tng-sp-ia-wtapi:v5.0'
            sh 'docker push sonatanfv/tng-sp-ia-wtapi:v5.0'
          }
        }
        stage('Deploying in v5.0 servers') {
          steps {
            sh 'rm -rf tng-devops || true'
            sh 'git clone https://github.com/sonata-nfv/tng-devops.git'
            dir(path: 'tng-devops') {
              sh 'ansible-playbook roles/sp.yml -i environments -e "target=sta-sp-v5-0 component=infrastructure-abstraction"'
              sh 'ansible-playbook roles/vnv.yml -i environments -e "target=sta-vnv-v5-0 component=infrastructure-abstraction"'
            }
          }
        }
      }
    }
  }
  post {
    always {
      echo 'TODO'
    }
  }
}