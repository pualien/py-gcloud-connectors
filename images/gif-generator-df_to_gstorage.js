const generateGif = require('code-gif-generator');
const fs = require('fs');
const path = require('path');

const createReadmeGif = async () => {
   // get the content of the README file
   const readmeContent = await fs.promises.readFile('images/df_to_gstorage.py', 'utf8');
   // create a GIF from the readme file
   const gif = await generateGif(readmeContent, {
    preset: 'ultra',   // scroll slowly, up to 250 frames
    mode: 'python',   // pass the snippet programming language
    theme: 'darcula',   // theme for the code editor
    lineNumbers: false, // hide line numbers
  });
   // save the GIF in the docs/img folder
   const gifPath = await gif.save('df-to-gstorage', path.resolve(__dirname), "lossless");
   return gifPath;
}

createReadmeGif().then(gifPath => console.log(`Gif saved: ${gifPath}`));