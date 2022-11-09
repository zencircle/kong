pipeline {
    agent none
    options {
        retry(1)
        timeout(time: 2, unit: 'HOURS')
    }
    environment {
        UPDATE_CACHE = "true"
        DOCKER_CREDENTIALS = credentials('dockerhub')
        DOCKER_USERNAME = "${env.DOCKER_CREDENTIALS_USR}"
        DOCKER_PASSWORD = "${env.DOCKER_CREDENTIALS_PSW}"
        DOCKER_CLI_EXPERIMENTAL = "enabled"
        // PULP_PROD and PULP_STAGE are used to do releases
        PULP_HOST_PROD = "https://api.pulp.konnect-prod.konghq.com"
        PULP_PROD = credentials('PULP')
        PULP_HOST_STAGE = "https://api.pulp.konnect-stage.konghq.com"
        PULP_STAGE = credentials('PULP_STAGE')
        GITHUB_TOKEN = credentials('github_bot_access_token')
        DEBUG = 0
    }
    stages {
        stage('Release -- Tag Release to Official Asset Stores') {
            when {
                beforeAgent true
                allOf {
                    branch '3.0.1-arm64-focal'
                    not { triggeredBy 'TimerTrigger' }
                }
            }
            parallel {
                stage('DEB') {
                    agent {
                        node {
                            label 'bionic'
                        }
                    }
                    environment {
                        KONG_SOURCE_LOCATION = "${env.WORKSPACE}"
                        KONG_BUILD_TOOLS_LOCATION = "${env.WORKSPACE}/../kong-build-tools"
                        PACKAGE_TYPE = "deb"
                        GITHUB_SSH_KEY = credentials('github_bot_ssh_key')
                        AWS_ACCESS_KEY = "instanceprofile"
                    }
                    steps {
                        sh 'echo "$DOCKER_PASSWORD" | docker login -u "$DOCKER_USERNAME" --password-stdin || true'
                        sh 'make setup-kong-build-tools'
                        sh 'make RESTY_IMAGE_BASE=ubuntu RESTY_IMAGE_TAG=20.04 RELEASE_DOCKER=true DOCKER_MACHINE_ARM64_NAME="kong-"`cat /proc/sys/kernel/random/uuid` release'
                    }
                }
            }
        }
    }
}
