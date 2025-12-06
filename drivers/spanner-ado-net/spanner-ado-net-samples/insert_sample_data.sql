/* Copyright 2025 Google LLC
* 
* Licensed under the Apache License, Version 2.0 (the "License")
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
* 
*     https://www.apache.org/licenses/LICENSE-2.0
* 
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

INSERT OR UPDATE INTO Singers (SingerId, FirstName, LastName, BirthDate, Picture) VALUES 
  (1, 'Mark', 'Richards', DATE '1990-11-09', NULL),
  (2, 'Catalina', 'Smith', DATE '1998-04-29', NULL),
  (3, 'Alice', 'Trentor', DATE '1979-10-15', NULL),
  (4, 'Lea', 'Martin', NULL, NULL);
