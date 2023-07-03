#
# Copyright (c) 2023, Neptune Labs Sp. z o.o.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
A minimalistic example with a toy dataset to see if tracking works in a GCP environment with low memory.
"""

from datetime import (
    datetime,
    timedelta,
)

import tensorflow as tf
from airflow import DAG
from airflow.decorators import task
from neptune.integrations.tensorflow_keras import NeptuneCallback
from neptune.types import File

from neptune_airflow import NeptuneLogger


def train_model(logger: NeptuneLogger, **context):
    handler = logger.get_task_handler_from_context(context=context)
    run = handler.get_root_object()
    x_train = tf.random.uniform(shape=[2, 28, 28])
    y_train = tf.constant([1, 1], shape=(2, 1), dtype=tf.int8)

    model = tf.keras.models.Sequential(
        [
            tf.keras.layers.Flatten(),
            tf.keras.layers.Dense(256, activation=tf.keras.activations.relu),
            tf.keras.layers.Dropout(0.5),
            tf.keras.layers.Dense(10, activation=tf.keras.activations.softmax),
        ]
    )

    optimizer = tf.keras.optimizers.SGD(
        learning_rate=0.005,
        momentum=0.4,
    )

    model.compile(optimizer=optimizer, loss="sparse_categorical_crossentropy", metrics=["accuracy"])

    # (Neptune) log metrics during training
    neptune_cbk = NeptuneCallback(run=handler)
    model.fit(x_train, y_train, epochs=5, batch_size=64, callbacks=[neptune_cbk])
    model.save("my_model.h5")
    run["model_checkpoint/checkpoint"].upload_files("my_model.h5")
    run.sync()
    run.stop()


def evaluate_model(logger: NeptuneLogger, **context):
    handler = logger.get_task_handler_from_context(context=context)
    run = handler.get_root_object()

    # if the tasks don't share the same file system
    # run["model_checkpoint/checkpoint/my_model.h5"].download()

    model = tf.keras.models.load_model("my_model.h5")
    x_test = tf.random.uniform(shape=[2, 28, 28])
    y_test = tf.constant([1, 1], shape=(2, 1), dtype=tf.int8)

    for image, label in zip(x_test[:10], y_test[:10]):
        prediction = model.predict(image[None], verbose=0)
        predicted = prediction.argmax()
        desc = f"label : {label} | predicted : {predicted}"
        handler["visualization/test_prediction"].append(File.as_image(image), description=desc)
    run.sync()
    run.stop()


with DAG(
    dag_id="test_dag",
    description="test_description",
    tags=["neptune", "tensorflow"],
    schedule="@daily",
    start_date=datetime.today() - timedelta(days=1),
    catchup=False,
) as dag:

    @task(task_id="train-model")
    def task1(**context):
        logger = NeptuneLogger()
        return train_model(logger, **context)

    @task(task_id="evaluate-model")
    def task2(**context):
        logger = NeptuneLogger()
        return evaluate_model(logger, **context)

    task1() >> task2()
