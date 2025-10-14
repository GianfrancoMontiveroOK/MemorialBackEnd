import app from "./app.js";
import { connectDB } from "./db.js";

connectDB();


const PORT = process.env.NODE_ENV === "production"
  ? process.env.PORT
  : 4000;


app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});

