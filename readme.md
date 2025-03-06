
# Installing Kafka for PHP with rdkafka

This guide explains how to install and configure the **rdkafka** extension in PHP 8.3, which is a required dependency for the **php-kafka-client** library. This setup enables interaction with Apache Kafka in your PHP applications.

## 1. Install Dependencies

First, install the **librdkafka** library, which is required for the extension to work:

```bash  
sudo apt install librdkafka-dev
```  

## 2. Install the rdkafka Extension

Now, install the **rdkafka** extension using **PECL**:

```bash  
sudo pecl install rdkafka
```  

## 3. Enable the Extension in PHP

After installation, add the extension to PHP by creating a configuration file:

```bash  
echo "extension=rdkafka.so" | sudo tee /etc/php/8.3/mods-available/rdkafka.ini
```  

Now, enable the extension with:

```bash  
sudo phpenmod -v 8.3 rdkafka
```  

## 4. Verify if the Extension is Active

To check if **rdkafka** is correctly installed, run:

```bash  
php -m | grep rdkafka
```  

If the extension is installed correctly, the command above should display **rdkafka** in the output.

Now your PHP environment is ready to use **php-kafka-client** to interact with **Apache Kafka**! 🚀
