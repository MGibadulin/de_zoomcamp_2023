from prefect.infrastructure.docker import DockerContainer

# alternative to creating DockerContainer block in the UI
docker_block = DockerContainer(
    image="w2_test:v001",  # insert your image here
    image_pull_policy="NEVER",
    auto_remove=True,
)

docker_block.save("zoom", overwrite=True)